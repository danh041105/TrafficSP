// Sơ đồ pipeline (SVG). Node tô theo trạng thái sức khoẻ do API tính từ số liệu thật; không có hiệu ứng "dữ liệu đang chạy".
import { ago, int, num } from "../../lib";
import { HEALTH, type Flow } from "./types";

const COL = 156, ROW = 100, W = 142, H = 70, PAD = 14;
// Nhánh Speed ở hàng trên, nhánh Batch ở hàng dưới; Kafka và Trino nằm giữa hai nhánh.
const POS: Record<string, [number, number]> = {
  kafka: [0, 0.5], speed: [1, 0], iceberg_speed: [2.5, 0], trino: [6, 0.5], inference: [7, 0], dashboard: [8, 0.5],
  bronze: [1, 1], spark_batch: [2, 1], silver: [3, 1], gold: [4, 1], handoff: [5, 1],
};
const KEY_METRIC: Record<string, (m: Record<string, any>) => string> = {
  kafka: (m) => (m.messages_per_s != null ? `${num(m.messages_per_s, 1)} msg/s` : "—"),
  speed: (m) => (m.input_rows_per_s != null ? `${num(m.input_rows_per_s, 0)} dòng/s` : "—"),
  iceberg_speed: (m) => (m.records != null ? `${int(Number(m.records))} cửa sổ` : "—"),
  trino: (m) => (m.query_ms_p50 != null ? `p50 ${int(m.query_ms_p50)} ms` : "—"),
  inference: (m) => m.last_status ?? "—",
  dashboard: (m) => (m.warm_seconds != null ? `làm ấm ${num(m.warm_seconds, 0)} s` : "—"),
  bronze: (m) => (m.records != null ? `${int(Number(m.records))} dòng` : "—"),
  spark_batch: (m) => m.last_state ?? "—",
  silver: (m) => (m.records != null ? `${int(Number(m.records))} dòng` : "—"),
  gold: (m) => (m.records != null ? `${int(Number(m.records))} cửa sổ` : "—"),
  handoff: (m) => m.last_status ?? "—",
};

export function FlowDiagram({ flow, selected, onSelect }: { flow: Flow; selected: string | null; onSelect: (k: string) => void }) {
  const xy = (k: string) => [PAD + POS[k][0] * COL, PAD + POS[k][1] * ROW] as const;
  const width = PAD * 2 + 8 * COL + W, height = PAD * 2 + ROW + H;
  return (
    <svg className="flow-svg" viewBox={`0 0 ${width} ${height}`} role="group" aria-label="Sơ đồ pipeline Speed và Batch">
      <defs>
        <marker id="arr" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L8,4 L0,8 z" fill="#9aa0a6" />
        </marker>
      </defs>
      <text x={PAD} y={PAD + 4} className="flow-lane-label">Nhánh Speed</text>
      <text x={PAD} y={height - 2} className="flow-lane-label">Nhánh Batch</text>
      {flow.edges.filter(([a, b]) => POS[a] && POS[b]).map(([a, b]) => {
        const [ax, ay] = xy(a), [bx, by] = xy(b);
        const x1 = ax + W, y1 = ay + H / 2, x2 = bx, y2 = by + H / 2;
        const mx = (x1 + x2) / 2;
        return <path key={a + b} d={`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2 - 2},${y2}`} fill="none" stroke="#b9bdb5" strokeWidth={1.4} markerEnd="url(#arr)" />;
      })}
      {Object.entries(POS).map(([k]) => {
        const n = flow.nodes[k];
        if (!n) return null;
        const [x, y] = xy(k);
        const h = HEALTH[n.status] ?? HEALTH.unknown;
        const tip = `${n.label} - ${h.label}\n${n.sub}\n` + Object.entries(n.metrics).map(([mk, mv]) => `${mk}: ${mv ?? "không có"}`).join("\n")
          + (n.last_update ? `\ncập nhật ${ago(n.last_update)} trước` : "");
        return (
          <g key={k} className={`flow-node${selected === k ? " selected" : ""}`} transform={`translate(${x},${y})`} tabIndex={0} role="button"
            aria-label={`${n.label}: ${h.label}`} onClick={() => onSelect(k)} onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && onSelect(k)}>
            <title>{tip}</title>
            <rect width={W} height={H} rx={4} className="flow-box" />
            <rect width={4} height={H} rx={2} fill={h.color} />
            <text x={12} y={18} className="flow-name">{n.label}</text>
            <text x={12} y={37} className="flow-metric">{KEY_METRIC[k]?.(n.metrics) ?? "—"}</text>
            {/* trạng thái luôn có chữ, không chỉ dựa vào màu */}
            <text x={12} y={56} className="flow-time">
              {n.status !== "ok" ? <tspan className="flow-status" fill={n.status === "degraded" ? "#7a5200" : h.color}>{h.label}. </tspan> : null}
              {n.last_update ? `${ago(n.last_update)} trước` : "chưa có cập nhật"}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
