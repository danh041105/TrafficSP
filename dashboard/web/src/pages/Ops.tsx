// Trung tâm vận hành: timeline Batch/Speed (giờ sự kiện + giờ xử lý), sơ đồ pipeline, số đo thật, kiểm tra handoff và phiên phát lại.
import { Fragment, useState } from "react";
import { useApp } from "../App";
import { Loadable } from "../components/ui";
import { ago, duration, int, minutesBetween, nyHour, nyTime, num, useData, vnTime } from "../lib";
import { FlowDiagram } from "./ops/FlowDiagram";
import { GrafanaPanel } from "./ops/GrafanaPanel";
import { HandoffInspector, ReplayInspector } from "./ops/Inspectors";
import { MetricsGrid } from "./ops/MetricsGrid";
import { BatchStairs, WindowTimeline } from "./ops/Timeline";
import { HANDOFF_STATUS, HEALTH, LAYER, type Flow, type Handoffs, type Metrics, type Timeline } from "./ops/types";

type Legacy = {
  airflow: { dag_id: string; is_paused?: boolean; schedule?: string; error?: string; runs?: { run_id: string; state: string; start: string; end: string | null }[] }[] | { error: string };
  spark: { status: string; workers: { id: string; state: string; cores: number; cores_used: number; memory_mb: number; memory_used_mb: number; last_heartbeat_ms: number }[];
    apps: { name: string; state: string; cores: number; duration_ms: number }[]; error?: string };
  kafka: { targets: { job: string; instance: string; health: string; last_error: string | null }[]; error?: string };
  trino_ping_ms: number | { error: string };
};

const tone = (s?: string | null) => (["success", "up", "ALIVE", "PUBLISHED", "RUNNING", "running", "ok", "SUCCESS"].includes(s ?? "") ? "ok"
  : ["failed", "down", "FAILED", "DEAD", "error", "upstream_failed"].includes(s ?? "") ? "bad" : "warn");

export default function Ops() {
  const { lane, meta } = useApp();
  const tl = useData<Timeline>(`/api/ops/timeline?lane=${lane}&before_hours=2`, 15000);
  const ho = useData<Handoffs>(`/api/ops/handoffs?lane=${lane}`, 15000);
  const fl = useData<Flow>(`/api/ops/flow?lane=${lane}`, 15000);
  const mx = useData<Metrics>(`/api/ops/metrics?lane=${lane}&minutes=90`, 20000);
  const legacy = useData<Legacy>("/api/ops", 30000);
  const [node, setNode] = useState<string | null>(null);
  const [handoff, setHandoff] = useState<string | null>(null);
  const [asTable, setAsTable] = useState(false);

  const t = tl.data;
  const lastPub = ho.data?.handoffs.find((h) => h.status === "PUBLISHED");
  const servingSpeedMin = t?.batch_state && t.last_speed_window_utc ? minutesBetween(t.batch_state, t.last_speed_window_utc) : null;
  const kafkaRate = fl.data?.nodes.kafka?.metrics.messages_per_s;
  const sparkRate = fl.data?.nodes.speed?.metrics.input_rows_per_s;
  const inf = fl.data?.nodes.inference;
  const replay = lane === "demo" ? meta?.replay ?? null : null;
  const latestHandoff = ho.data?.handoffs[0];

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Trung tâm vận hành</h1>
        </div>
        <span className="muted">{t ? (lane === "demo" ? `Phiên ${t.run_id}` : "Hệ thống chính") : ""}</span>
      </div>

      <div className="ops-kpis" role="list">
        <div role="listitem"><span className="k">batch_state hiện tại</span><span className="v">{t?.batch_state ? nyTime(t.batch_state) : "—"}</span>
          <span className="d">giờ sự kiện, New York</span></div>
        <div role="listitem"><span className="k">Speed đang phục vụ</span>
          <span className="v">{servingSpeedMin != null && servingSpeedMin >= 0 ? duration(servingSpeedMin + 5) : "—"}</span>
          <span className="d">{t?.last_speed_window_utc ? `tới cửa sổ ${nyHour(t.last_speed_window_utc)}` : "chưa có cửa sổ Speed"}</span></div>
        <div role="listitem"><span className="k">Handoff gần nhất</span>
          <span className="v">{latestHandoff ? <span style={{ color: HANDOFF_STATUS[latestHandoff.status]?.color }}>{HANDOFF_STATUS[latestHandoff.status]?.label}</span> : "—"}</span>
          <span className="d">{lastPub ? `công bố ${ago(lastPub.finished_at)} trước` : ho.data && !ho.data.batch_lane ? "làn không có Batch riêng" : "chưa công bố lần nào"}</span></div>
        <div role="listitem"><span className="k">Kafka</span><span className="v">{kafkaRate != null ? num(kafkaRate, 1) : "—"}<small>msg/s</small></span>
          <span className="d">trung bình 1 phút, Prometheus</span></div>
        <div role="listitem"><span className="k">Spark Speed Layer</span><span className="v">{sparkRate != null ? num(sparkRate, 0) : "—"}<small>dòng/s</small></span>
          <span className="d">{fl.data?.nodes.speed?.last_update ? `lô cuối ${ago(fl.data.nodes.speed.last_update)} trước` : "chưa có lô"}</span></div>
        <div role="listitem"><span className="k">Suy luận ML</span><span className="v"><span className={tone(inf?.metrics.last_status)}>{inf?.metrics.last_status ?? "—"}</span></span>
          <span className="d">{inf?.last_update ? `${ago(inf.last_update)} trước` : "chưa có lượt"}</span></div>
      </div>

      <section className="ops-block">
        <header><h2>Batch và Speed theo cửa sổ 5 phút</h2>
          <div className="controls">
            <div className="ops-legend">
              <span><i style={{ background: LAYER.batch }} />Batch đã công bố</span><span><i style={{ background: LAYER.speed }} />Speed</span>
              <span><i style={{ background: LAYER.batchAfter }} />Batch sau batch_state (chưa phục vụ)</span><span><i style={{ background: LAYER.superseded }} />Speed đã được Batch thay</span><span><i style={{ background: LAYER.pending }} />Chưa hoàn tất</span>
            </div>
            <button aria-pressed={asTable} onClick={() => setAsTable(!asTable)}>{asTable ? "Xem biểu đồ" : "Xem dạng bảng"}</button>
          </div>
        </header>
        <Loadable loading={tl.loading && !t} error={tl.error} empty={t?.empty ?? false}>
          {t && (asTable ? <WindowTable t={t} /> : <WindowTimeline data={t} />)}
        </Loadable>
        <h3 className="sub-h">batch_state và tiến độ Speed theo giờ xử lý</h3>
        <Loadable loading={tl.loading && !t} error={tl.error} empty={t?.empty ?? false}>
          {t && <BatchStairs data={t} />}
        </Loadable>
      </section>

      <section className="ops-block">
        <header><h2>Sơ đồ pipeline</h2></header>
        <Loadable loading={fl.loading && !fl.data} error={fl.error}>
          {fl.data && <div className="flow-wrap"><FlowDiagram flow={fl.data} selected={node} onSelect={(k) => setNode(node === k ? null : k)} /></div>}
          <div className="ops-legend" style={{ marginTop: 6 }}>
            {Object.values(HEALTH).map((h) => <span key={h.label}><i style={{ background: h.color }} />{h.label}</span>)}
          </div>
          {fl.data && node && fl.data.nodes[node] && <NodeDetail k={node} n={fl.data.nodes[node]} onClose={() => setNode(null)} />}
        </Loadable>
      </section>

      <section className="ops-block">
        <header><h2>Thông lượng và độ trễ</h2></header>
        <Loadable loading={mx.loading && !mx.data} error={mx.error}>{mx.data && <MetricsGrid m={mx.data} />}</Loadable>
      </section>

      <section className="ops-block">
        <header><h2>Grafana: tài nguyên, Kafka, Spark</h2></header>
        <GrafanaPanel lane={lane} />
      </section>

      <section className="ops-block">
        <header><h2>Kiểm tra handoff</h2><span className="note">{ho.data?.control_table ? ho.data.control_table : ""}</span></header>
        <Loadable loading={ho.loading && !ho.data} error={ho.error}>
          {ho.data && <HandoffInspector data={ho.data} lane={lane} selected={handoff} onSelect={setHandoff} />}
        </Loadable>
      </section>

      <section className="ops-block">
        <header><h2>Phiên phát lại</h2></header>
        {lane === "main" ? <div className="empty">Hệ thống chính đã nhận hết dữ liệu năm 2025. Chọn "Phiên phát lại kiểm chứng" ở thanh bên để xem phiên phát lại.</div>
          : <ReplayInspector replay={replay} timeline={t ? { last_speed_window_utc: t.last_speed_window_utc, batch_state: t.batch_state } : null} />}
      </section>

      <section className="ops-block">
        <header><h2>Chi tiết thành phần</h2></header>
        <Loadable loading={legacy.loading && !legacy.data} error={legacy.error}>{legacy.data && <Components d={legacy.data} />}</Loadable>
      </section>
    </>
  );
}

function WindowTable({ t }: { t: Timeline }) {
  return (
    <div className="scroll" style={{ maxHeight: 320 }}>
      <table className="data">
        <thead><tr><th>Cửa sổ (NY)</th><th className="num">Batch đã công bố</th><th className="num">Speed</th><th>Đang phục vụ</th><th>Ghi chú</th></tr></thead>
        <tbody>{[...t.windows].reverse().map((w) => (
          <tr key={w.t}><td>{nyTime(w.t)}</td><td className="num">{w.batch ?? "—"}</td><td className="num">{w.speed ?? "—"}</td>
            <td>{w.serving_batch ? `Batch, ${w.serving_batch} đoạn` : w.serving_speed ? `Speed, ${w.serving_speed} đoạn` : "—"}</td>
            <td className="muted">{w.superseded ? "Speed đã được Batch thay" : w.pending ? "chưa hoàn tất" : ""}</td></tr>))}
        </tbody>
      </table>
    </div>
  );
}

function NodeDetail({ k, n, onClose }: { k: string; n: Flow["nodes"][string]; onClose: () => void }) {
  const h = HEALTH[n.status];
  const recent = n.detail?.recent ?? n.detail?.supervisor ?? n.detail?.runs ?? null;
  return (
    <div className="node-detail" role="region" aria-label={`Chi tiết ${n.label}`}>
      <div className="node-detail-head"><b>{n.label}</b><span className="chip"><i style={{ background: h.color }} />{h.label}</span>
        <span className="muted">{n.sub}</span><button onClick={onClose}>Đóng</button></div>
      <dl className="kv compact">
        {Object.entries(n.metrics).map(([mk, mv]) => <Fragment key={mk}><dt>{METRIC_LABEL[mk] ?? mk}</dt><dd>{fmt(mv)}</dd></Fragment>)}
        <dt>Cập nhật</dt><dd>{n.last_update ? `${vnTime(n.last_update)} giờ VN (${ago(n.last_update)} trước)` : "chưa có"}</dd>
      </dl>
      {Array.isArray(recent) && recent.length > 0 && (
        <div className="scroll" style={{ maxHeight: 220 }}>
          <table className="data"><tbody>{recent.slice(-8).map((r: any, i: number) => (
            <tr key={i}><td className="muted" style={{ whiteSpace: "pre-wrap" }}>{typeof r === "string" ? r : summarize(k, r)}</td></tr>))}</tbody></table>
        </div>)}
    </div>
  );
}

const METRIC_LABEL: Record<string, string> = {
  messages_per_s: "Message/giây", offset_total: "Tổng offset topic", input_rows_per_s: "Dòng vào/giây", micro_batch_ms: "Micro-batch (ms)",
  offsets_behind: "Offset chưa đọc", supervisor_restarts: "Lần supervisor khởi động lại (log gần đây)", records: "Số dòng", added_last_commit: "Thêm ở commit cuối",
  query_ms_p50: "Truy vấn p50 (ms)", errors: "Truy vấn lỗi", queries: "Số truy vấn đã đo", last_status: "Kết quả gần nhất", seconds: "Thời lượng (giây)",
  failures_recent: "Lỗi trong 12 lượt gần nhất", warm_seconds: "Làm ấm cache (giây)", last_state: "Lượt gần nhất", duration_s: "Thời lượng (giây)",
  paused: "Đang tạm dừng", batch_state: "batch_state",
};
const fmt = (v: any) => (v == null ? "không có số liệu" : typeof v === "number" ? num(v, Number.isInteger(v) ? 0 : 1) : v === true ? "có" : v === false ? "không"
  : /^\d{4}-\d\d-\d\dT/.test(String(v)) ? nyTime(String(v)) : /^\d+(\.\d+)?$/.test(String(v)) ? int(Number(v)) : String(v));
const summarize = (k: string, r: any) => k === "trino" ? `${vnTime(r.at)}  ${r.ms} ms  ${r.ok ? "" : "LỖI "}${r.sql}`
  : k === "inference" ? `${vnTime(r.started_at)}  ${r.status}  ${r.seconds ?? "—"} s  ${r.reason ?? ""}`
  : k === "handoff" ? `${r.handoff_id}  ${r.status}  ${r.batch_state ? nyTime(r.batch_state) : ""}`
  : k === "spark_batch" ? `${r.run_id}  ${r.state}  ${r.duration_s ?? "—"} s` : JSON.stringify(r);

function Components({ d }: { d: Legacy }) {
  const af = "error" in (d.airflow as any) ? [] : (d.airflow as Exclude<Legacy["airflow"], { error: string }>);
  return (
    <div className="grid-3 components">
      <div><h3 className="sub-h">Airflow</h3><table className="data"><tbody>{af.map((x) => (
        <tr key={x.dag_id}><td>{x.dag_id}{x.is_paused ? <div className="muted">tạm dừng</div> : null}</td>
          <td><span className={tone(x.runs?.[0]?.state)}>{x.error ?? x.runs?.[0]?.state ?? "—"}</span></td>
          <td className="muted">{x.runs?.[0]?.start ? vnTime(x.runs[0].start) : ""}</td></tr>))}</tbody></table></div>
      <div><h3 className="sub-h">Spark</h3><table className="data"><tbody>
        {(d.spark?.workers ?? []).map((w) => <tr key={w.id}><td>worker</td><td><span className={tone(w.state)}>{w.state}</span></td><td className="num">{w.cores_used}/{w.cores} core</td></tr>)}
        {(d.spark?.apps ?? []).map((a) => <tr key={a.name + a.duration_ms}><td>{a.name}</td><td><span className={tone(a.state)}>{a.state}</span></td><td className="num">{duration(Math.round(a.duration_ms / 60000))}</td></tr>)}
        {d.spark && !d.spark.workers?.length && <tr><td colSpan={3} className="bad">Không có worker nào đăng ký với master.</td></tr>}
      </tbody></table></div>
      <div><h3 className="sub-h">Prometheus targets</h3><table className="data"><tbody>{(d.kafka?.targets ?? []).map((x) => (
        <tr key={x.job + x.instance}><td>{x.job}</td><td><span className={tone(x.health)}>{x.health}</span></td>
          <td className="muted">{x.health === "down" ? (x.instance.includes(":4040") ? "chỉ có khi một job batch đang chạy" : x.instance.includes("worker-2") ? "worker-2 không chạy" : "") : ""}</td></tr>))}
      </tbody></table><p className="muted" style={{ fontSize: 12 }}>Trino phản hồi {typeof d.trino_ping_ms === "number" ? `${int(d.trino_ping_ms)} ms` : "lỗi"}.</p></div>
    </div>
  );
}
