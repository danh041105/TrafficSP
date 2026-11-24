// Kiểm tra handoff (lịch sử từ lambda_control, trước/sau theo snapshot đã công bố) và kiểm tra phiên phát lại.
import { useMemo } from "react";
import { axisStyle, baseChart, Chart, INK, Loadable } from "../../components/ui";
import { duration, int, minutesBetween, nyHour, nyTime, num, useData, vnTime } from "../../lib";
import { HANDOFF_STATUS, LAYER, type Handoff, type Handoffs } from "./types";

const Chip = ({ s }: { s: string }) => {
  const m = HANDOFF_STATUS[s] ?? { color: "#9aa0a6", label: s };
  return <span className="chip"><i style={{ background: m.color }} />{m.label}</span>;
};
const check = (v: any) => (v === true ? <span className="ok">đạt</span> : v === false ? <span className="bad">không đạt</span> : <span className="muted">—</span>);

export function HandoffInspector({ data, lane, selected, onSelect }: { data: Handoffs; lane: string; selected: string | null; onSelect: (id: string) => void }) {
  const cur = data.handoffs.find((h) => h.handoff_id === selected) ?? data.handoffs[0];
  const lastPub = data.handoffs.find((h) => h.status === "PUBLISHED");
  const prevPub = data.handoffs.filter((h) => h.status === "PUBLISHED")[1];
  if (!data.batch_lane) return <div className="empty">Làn này không có Batch Layer riêng nên không có handoff. batch_state cố định ở mốc bắt đầu phiên ({nyTime(data.batch_state)} New York).</div>;
  return (
    <div className="handoff-layout">
      <div>
        <dl className="kv compact">
          <dt>batch_state hiện tại</dt><dd><b>{nyTime(data.batch_state)}</b> <span className="muted">giờ sự kiện, New York</span></dd>
          <dt>batch_state trước đó</dt><dd>{lastPub?.batch_state_prev ? nyTime(lastPub.batch_state_prev) : "— (lượt công bố đầu tiên)"}</dd>
          <dt>Công bố gần nhất</dt><dd>{lastPub ? <>{vnTime(lastPub.finished_at)} <span className="muted">giờ xử lý, VN</span></> : "chưa có"}</dd>
          <dt>Snapshot Batch 5 phút</dt><dd className="mono-id">{lastPub?.snapshot_5min ?? "—"}</dd>
          <dt>Lần trước nữa</dt><dd>{prevPub ? `${nyTime(prevPub.batch_state)} (công bố ${vnTime(prevPub.finished_at)})` : "—"}</dd>
        </dl>
        <div className="scroll" style={{ maxHeight: 330, marginTop: 10 }}>
          <table className="data">
            <thead><tr><th>Handoff</th><th>Kết quả</th><th>Mốc cắt sự kiện (NY)</th><th>Xử lý lúc (VN)</th><th className="num">Thời lượng</th><th className="num">Cửa sổ công bố</th></tr></thead>
            <tbody>{data.handoffs.map((h) => (
              <tr key={h.handoff_id} className={`clickable${cur?.handoff_id === h.handoff_id ? " selected" : ""}`} onClick={() => onSelect(h.handoff_id)}>
                <td className="mono-id">{h.handoff_id.replace("handoff-", "")}</td>
                <td><Chip s={h.status} /></td>
                <td>{h.batch_state_prev ? nyHour(h.batch_state_prev) : "đầu"} → {h.batch_state ? nyTime(h.batch_state) : "—"}</td>
                <td>{vnTime(h.started_at)}</td>
                <td className="num">{h.duration_s != null ? `${num(h.duration_s, 0)} s` : "—"}</td>
                <td className="num">{int(h.published_windows)}</td>
              </tr>))}
            </tbody>
          </table>
        </div>
      </div>
      {cur ? <HandoffDetail h={cur} lane={lane} /> : <div className="empty">Chưa có handoff nào trong {data.control_table}.</div>}
    </div>
  );
}

function HandoffDetail({ h, lane }: { h: Handoff; lane: string }) {
  const { data, error, loading } = useData<{ batch_after: { t: string; n: number }[]; speed_before: { t: string; n: number }[]; range: [string, string]; note: string }>(
    h.batch_state ? `/api/ops/handoffs/${encodeURIComponent(h.handoff_id)}/windows?lane=${lane}` : null);
  const option = useMemo(() => {
    if (!data) return null;
    const keys = Array.from(new Set([...data.batch_after, ...data.speed_before].map((x) => new Date(x.t).getTime()))).sort((a, b) => a - b);
    const b = new Map(data.batch_after.map((x) => [new Date(x.t).getTime(), x.n]));
    const s = new Map(data.speed_before.map((x) => [new Date(x.t).getTime(), x.n]));
    return {
      ...baseChart(), legend: { top: 0, left: 0, icon: "rect", itemWidth: 10, itemHeight: 10 },
      grid: { left: 40, right: 10, top: 30, bottom: 26 },
      xAxis: { type: "category", data: keys.map((k) => nyHour(new Date(k).toISOString())), ...axisStyle, axisLabel: { ...axisStyle.axisLabel, interval: 11 } },
      yAxis: { type: "value", name: "đoạn", nameTextStyle: { color: INK.ink3, fontSize: 11 }, ...axisStyle },
      series: [
        { name: "Trước: Speed phục vụ", type: "bar", barGap: "0%", barCategoryGap: "20%", itemStyle: { color: LAYER.speed }, data: keys.map((k) => s.get(k) ?? null) },
        { name: "Sau: Batch (snapshot công bố)", type: "bar", itemStyle: { color: LAYER.batch }, data: keys.map((k) => b.get(k) ?? null) }],
    };
  }, [data]);
  const c = h.checks ?? {};
  return (
    <div>
      <h3 className="sub-h">{h.handoff_id} <Chip s={h.status} /></h3>
      <div className="event-steps">
        {h.events.map((e, i) => <div key={i}><Chip s={e.status} /><span>{vnTime(e.at)}</span></div>)}
      </div>
      <p className="muted" style={{ fontSize: 12 }}>
        Mốc cắt giờ sự kiện {h.batch_state_prev ? nyTime(h.batch_state_prev) : "(đầu phiên)"} → {nyTime(h.batch_state)} New York;
        xử lý từ {vnTime(h.started_at)} tới {vnTime(h.finished_at)} giờ VN ({h.duration_s != null ? `${num(h.duration_s, 0)} giây` : "—"}).
      </p>
      <Loadable loading={loading && !data} error={error}>
        {option && <Chart option={option} height={190} label="Số đoạn theo cửa sổ: Speed trước và Batch sau handoff" />}
      </Loadable>
      <table className="data compact-table">
        <tbody>
          <tr><td>Silver trong khoảng</td><td className="num">{int(c.silver_rows)} bản ghi</td><td>Batch cộng lại</td><td className="num">{int(c.batch_sum_n_records)}</td></tr>
          <tr><td>Cửa sổ Batch</td><td className="num">{int(c.batch_rows)}</td><td>Khoá không trùng</td><td>{check(c.unique_grain)}</td></tr>
          <tr><td>Cửa sổ Speed đã so</td><td className="num">{int(c.rt_windows_compared)}</td><td>Lệch số bản ghi</td><td className="num">{int(c.rt_diff_records)}</td></tr>
          <tr><td>Speed không có Batch</td><td className="num">{int(c.rt_windows_without_batch)}</td><td>Phủ Speed</td><td>{check(c.covers_realtime)}</td></tr>
          <tr><td>Đủ dòng</td><td>{check(c.complete)}</td><td>Giờ khớp 5 phút</td><td>{check(c.hourly_consistent)}</td></tr>
        </tbody>
      </table>
      {h.message && <p className="bad" style={{ fontSize: 12 }}>{h.message}</p>}
    </div>
  );
}

export function ReplayInspector({ replay, timeline }: { replay: Record<string, any> | null; timeline: { last_speed_window_utc: string | null; batch_state: string | null } | null }) {
  if (!replay) return <div className="empty">Chưa có phiên phát lại nào. Chạy <code>python scripts/demo_replay.py start ...</code> (xem docs/demo_runbook.md).</div>;
  const st = String(replay.state ?? "").toUpperCase();
  const label: Record<string, string> = { RUNNING: "Đang phát", FINISHED: "Đã phát xong", STOPPED: "Đã dừng", FAILED: "Lỗi", STARTING: "Đang dựng làn",
    VIEWS_READY: "Đang dựng làn", SPEED_READY: "Đang dựng làn" };
  const s0 = replay.start_utc && new Date(replay.start_utc).getTime(), s1 = replay.end_utc && new Date(replay.end_utc).getTime();
  const at = replay.event_time_utc && new Date(replay.event_time_utc).getTime();
  const pct = s0 && s1 && at ? Math.min(1, Math.max(0, (at - s0) / (s1 - s0))) : null;
  const speedLag = minutesBetween(timeline?.last_speed_window_utc, replay.event_time_utc);
  const batchLag = minutesBetween(timeline?.batch_state, replay.event_time_utc);
  return (
    <div className="replay-grid">
      <div>
        <div className="replay-state">
          <span className={`chip big ${st === "RUNNING" ? "run" : ""}`}><i />{label[st] ?? st}</span>
          <span className="mono-id">{replay.run_id}</span>
        </div>
        <div className="progress" role="progressbar" aria-valuemin={0} aria-valuemax={100} aria-valuenow={pct != null ? Math.round(pct * 100) : undefined}>
          <span style={{ width: `${(pct ?? 0) * 100}%` }} />
        </div>
        <div className="muted" style={{ fontSize: 12, display: "flex", justifyContent: "space-between" }}>
          <span>{nyTime(replay.start_utc)}</span><span>{pct != null ? `${num(pct * 100, 0)}% khoảng giờ sự kiện` : ""}</span><span>{nyTime(replay.end_utc)}</span>
        </div>
        <p className="replay-note">Dữ liệu lịch sử NYC DOT năm 2025 được phát lại qua Kafka theo giờ sự kiện gốc. Đây không phải giao thông New York trực tiếp.</p>
      </div>
      <dl className="kv compact">
        <dt>Tốc độ phát</dt><dd>gấp {replay.speedup} lần thời gian thực</dd>
        <dt>Message đã gửi</dt><dd>{int(replay.sent)}{replay.failed ? <span className="bad">, lỗi {replay.failed}</span> : null}</dd>
        <dt>Event cuối đã phát</dt><dd>{nyTime(replay.event_time_utc)} <span className="muted">giờ sự kiện, NY</span></dd>
        <dt>Cập nhật trạng thái</dt><dd>{vnTime(replay.updated_at)} <span className="muted">giờ xử lý, VN</span></dd>
        <dt>Speed chậm hơn producer</dt><dd>{speedLag != null ? duration(speedLag) : "—"} <span className="muted">giờ sự kiện (cửa sổ Speed cuối)</span></dd>
        <dt>Batch chậm hơn producer</dt><dd>{batchLag != null ? duration(batchLag) : "—"} <span className="muted">giờ sự kiện (batch_state)</span></dd>
      </dl>
    </div>
  );
}
