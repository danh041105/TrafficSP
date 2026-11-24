// Lưới biểu đồ số đo thật. Chuỗi rỗng -> hiện "chưa có số liệu" kèm lý do, không vẽ đường 0.
import { useMemo, useState } from "react";
import { axisStyle, baseChart, Chart, INK, SERIES } from "../../components/ui";
import { vnTime } from "../../lib";
import { HANDOFF_STATUS, type Metrics } from "./types";

const t = (s: string) => new Date(s).getTime();
const sec = (ms: number | null | undefined) => (ms == null ? null : ms / 1000);
const timeAxis = { type: "time", ...axisStyle, splitNumber: 4, minInterval: 60000,
  axisLabel: { ...axisStyle.axisLabel, hideOverlap: true, formatter: (v: number) => vnTime(new Date(v).toISOString(), false) } };

// Ngắt đường khi hai mẫu cách nhau quá xa (dừng phát, máy ngủ): ECharts không nối qua giá trị null,
// nên không vẽ một đoạn thẳng giả qua khoảng không có số liệu.
const GAP_MS = 10 * 60 * 1000;
function breakGaps(d: [number, number | null][]): [number, number | null][] {
  const out: [number, number | null][] = [];
  d.forEach((p, i) => {
    if (i && p[0] - d[i - 1][0] > GAP_MS) out.push([(p[0] + d[i - 1][0]) / 2, null]);
    out.push(p);
  });
  return out;
}

function lineOpt(series: { name: string; data: [number, number | null][]; color: string; area?: boolean }[], unit: string) {
  return {
    ...baseChart(),
    legend: series.length > 1 ? { top: 0, left: 0, icon: "rect", itemWidth: 12, itemHeight: 3 } : undefined,
    grid: { left: 56, right: 12, top: series.length > 1 ? 46 : 24, bottom: 26 },
    tooltip: { trigger: "axis", backgroundColor: "#fff", borderColor: INK.axis, textStyle: { color: INK.ink, fontSize: 12 },
      valueFormatter: (v: number) => (v == null ? "không có" : `${Number(v).toLocaleString("vi-VN", { maximumFractionDigits: 1 })} ${unit}`) },
    xAxis: timeAxis,
    yAxis: { type: "value", name: unit, nameTextStyle: { color: INK.ink3, fontSize: 11 }, ...axisStyle, min: 0 },
    series: series.map((s) => ({ name: s.name, type: "line", showSymbol: s.data.length < 40, symbolSize: 5, data: breakGaps(s.data), connectNulls: false,
      lineStyle: { width: 2, color: s.color }, itemStyle: { color: s.color }, areaStyle: s.area ? { color: s.color, opacity: 0.08 } : undefined })),
  };
}

function Tile({ title, scope, children, empty }: { title: string; scope: string; children: React.ReactNode; empty?: string | false }) {
  return (
    <div className="metric-tile">
      <div className="metric-head"><h3>{title}</h3><span title={scope}>{scope}</span></div>
      {empty ? <div className="empty small">{empty}</div> : children}
    </div>
  );
}

export function MetricsGrid({ m }: { m: Metrics }) {
  const queries = Object.keys(m.streaming).sort();
  const [q, setQ] = useState(queries.includes("speed_5min") ? "speed_5min" : queries[0]);
  const sp = m.streaming[q] ?? [];
  const k = "rate" in (m.kafka as any) ? (m.kafka as Extract<Metrics["kafka"], { rate: any }>) : null;
  const dag = m.dag && "runs" in m.dag ? m.dag : null;

  const opts = useMemo(() => ({
    kafka: k && lineOpt([{ name: "message/giây", data: k.rate.map(([a, v]) => [t(a), v]), color: SERIES[0], area: true }], "msg/s"),
    rows: lineOpt([
      { name: "Dòng vào/giây", data: sp.map((p) => [t(p.ts), p.input_rows_per_sec]), color: SERIES[0] },
      { name: "Dòng xử lý/giây", data: sp.map((p) => [t(p.ts), p.processed_rows_per_sec]), color: SERIES[1] }], "dòng/s"),
    batch: lineOpt([{ name: "Thời lượng micro-batch", data: sp.map((p) => [t(p.ts), sec(p.trigger_execution_ms)]), color: SERIES[0] }], "giây"),
    behind: lineOpt([{ name: "Offset chưa đọc", data: sp.map((p) => [t(p.ts), p.max_offsets_behind == null ? null : Number(p.max_offsets_behind)]), color: SERIES[1], area: true }], "offset"),
    addb: lineOpt([{ name: "addBatch (ghi lô + commit Iceberg)", data: sp.filter((p) => p.add_batch_ms != null).map((p) => [t(p.ts), sec(p.add_batch_ms)]), color: SERIES[0] }], "giây"),
    lat: lineOpt([
      { name: "p50", data: m.latency.map((x) => [t(x.ts), x.p50_s]), color: SERIES[0] },
      { name: "p95", data: m.latency.map((x) => [t(x.ts), x.p95_s]), color: SERIES[1] }], "giây"),
    trino: {
      ...baseChart(), grid: { left: 56, right: 12, top: 46, bottom: 26 },
      legend: { top: 0, left: 0, itemWidth: 10, itemHeight: 10 },
      tooltip: { trigger: "item", backgroundColor: "#fff", borderColor: INK.axis, textStyle: { color: INK.ink, fontSize: 12 },
        formatter: (p: any) => `${vnTime(new Date(p.value[0]).toISOString())}<br/>${p.value[1]} ms<br/><span style="color:#7a838c">${p.data.sql}</span>` },
      xAxis: timeAxis, yAxis: { type: "log", name: "ms", nameTextStyle: { color: INK.ink3, fontSize: 11 }, ...axisStyle, min: 1 },
      series: [
        { name: "Thành công", type: "scatter", symbolSize: 6, itemStyle: { color: SERIES[0], opacity: 0.7 },
          data: m.trino.filter((x) => x.ok).map((x) => ({ value: [t(x.at), Math.max(1, x.ms)], sql: x.sql })) },
        { name: "Lỗi", type: "scatter", symbol: "diamond", symbolSize: 9, itemStyle: { color: HANDOFF_STATUS.FAILED.color },
          data: m.trino.filter((x) => !x.ok).map((x) => ({ value: [t(x.at), Math.max(1, x.ms)], sql: x.sql })) }],
    },
    dag: dag && {
      ...baseChart(), grid: { left: 52, right: 12, top: 12, bottom: 26 },
      tooltip: { trigger: "item", backgroundColor: "#fff", borderColor: INK.axis, textStyle: { color: INK.ink, fontSize: 12 },
        formatter: (p: any) => `${p.data.run_id}<br/>${p.data.state}, ${p.value[1] ?? "đang chạy"} giây` },
      xAxis: timeAxis, yAxis: { type: "value", name: "giây", nameTextStyle: { color: INK.ink3, fontSize: 11 }, ...axisStyle, min: 0 },
      series: [{ type: "bar", barMaxWidth: 14, data: [...dag.runs].reverse().filter((r) => r.duration_s != null).map((r) => ({
        value: [t(r.start), r.duration_s], run_id: r.run_id, state: r.state,
        itemStyle: { color: r.state === "success" ? SERIES[0] : r.state === "failed" ? HANDOFF_STATUS.FAILED.color : INK.ink3, borderRadius: [3, 3, 0, 0] } })) }],
    },
    inf: {
      ...baseChart(), grid: { left: 52, right: 12, top: 12, bottom: 26 },
      tooltip: { trigger: "item", backgroundColor: "#fff", borderColor: INK.axis, textStyle: { color: INK.ink, fontSize: 12 },
        formatter: (p: any) => `${vnTime(new Date(p.value[0]).toISOString())}<br/>${p.data.status}, ${p.value[1]} giây` + (p.data.reason ? `<br/>${p.data.reason}` : "") },
      xAxis: timeAxis, yAxis: { type: "value", name: "giây", nameTextStyle: { color: INK.ink3, fontSize: 11 }, ...axisStyle, min: 0 },
      series: [{ type: "bar", barMaxWidth: 10, data: m.inference.map((r) => ({ value: [t(r.started_at), r.seconds], status: r.status, reason: r.reason,
        itemStyle: { color: r.status === "SUCCESS" ? "#0ca30c" : r.status === "FAILED" ? HANDOFF_STATUS.FAILED.color : "#b6bbc0", borderRadius: [3, 3, 0, 0] } })) }],
    },
  }), [m, q]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <>
      <div className="controls" style={{ marginBottom: 10 }}>
        <label>Query Spark{" "}
          <select value={q} onChange={(e) => setQ(e.target.value)}>{queries.map((x) => <option key={x}>{x}</option>)}</select>
        </label>
      </div>
      <div className="metric-grid">
        <Tile title="Kafka message/giây" scope={`Prometheus, kafka-exporter, topic ${k?.topic ?? ""}`}
          empty={!k ? `Không đọc được Prometheus: ${(m.kafka as any).error}` : !k.rate.length && "Prometheus chưa có mẫu trong khoảng này."}>
          {opts.kafka && <Chart option={opts.kafka} height={170} label="Kafka message mỗi giây" />}</Tile>
        <Tile title="Spark: dòng/giây" scope={`StreamingQueryProgress, ${q}`} empty={!sp.length && "Query chưa có micro-batch nào."}>
          <Chart option={opts.rows} height={170} label="Dòng vào và xử lý mỗi giây" /></Tile>
        <Tile title="Thời lượng micro-batch" scope={`triggerExecution, ${q}`} empty={!sp.length && "Query chưa có micro-batch nào."}>
          <Chart option={opts.batch} height={170} label="Thời lượng micro-batch" /></Tile>
        <Tile title="Offset Kafka chưa đọc" scope="max_offsets_behind" empty={!sp.length && "Query chưa có micro-batch nào."}>
          <Chart option={opts.behind} height={170} label="Offset Kafka chưa đọc" /></Tile>
        <Tile title="Độ trễ Kafka tới Iceberg" scope="speed_latest, p50 / p95"
          empty={!m.latency.length && "Chưa có lô nào ghi phân vị thời điểm Kafka."}>
          <Chart option={opts.lat} height={170} label="Độ trễ xử lý p50 và p95" /></Tile>
        <Tile title="Ghi lô vào Iceberg" scope={`addBatch, ${q}`} empty={!sp.some((p) => p.add_batch_ms != null) && "Query không báo addBatch."}>
          <Chart option={opts.addb} height={170} label="Thời gian ghi lô vào Iceberg" /></Tile>
        <Tile title="Độ trễ truy vấn Trino" scope="backend dashboard" empty={!m.trino.length && "Backend chưa gửi truy vấn nào."}>
          <Chart option={opts.trino} height={170} label="Độ trễ truy vấn Trino" /></Tile>
        <Tile title="Thời lượng lượt DAG batch" scope={dag ? `Airflow, ${dag.dag_id}` : "không có làn batch"}
          empty={!dag ? "Làn này không có DAG batch." : !dag.runs.some((r) => r.duration_s != null) && "Chưa có lượt nào kết thúc."}>
          {opts.dag && <Chart option={opts.dag} height={170} label="Thời lượng các lượt DAG batch" />}
          <div className="mini-legend"><span><i style={{ background: SERIES[0] }} />Thành công</span><span><i style={{ background: "#d03b3b" }} />Lỗi</span><span><i style={{ background: INK.ink3 }} />Khác</span></div></Tile>
        <Tile title="Thời lượng suy luận" scope="inference_runs.jsonl, theo làn" empty={!m.inference.length && "Chưa có lượt suy luận nào cho làn này."}>
          <Chart option={opts.inf} height={170} label="Thời lượng các lượt suy luận" />
          <div className="mini-legend"><span><i style={{ background: "#0ca30c" }} />Thành công</span><span><i style={{ background: "#d03b3b" }} />Lỗi</span><span><i style={{ background: "#b6bbc0" }} />Bỏ qua</span></div></Tile>
      </div>
    </>
  );
}
