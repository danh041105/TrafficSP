// Kiểu dữ liệu của /api/ops/* (dashboard/api/ops_center.py) và màu ngữ nghĩa dùng chung trong trung tâm vận hành.

export type Win = { t: string; batch: number | null; speed: number | null; serving_batch: number | null; serving_speed: number | null;
  superseded: boolean; pending: boolean };
export type Stair = { at: string; batch_state: string | null; batch_state_prev: string | null; status: string; handoff_id: string };
export type Timeline = { lane: string; run_id: string | null; range: [string, string]; event_clock_utc: string; batch_state: string | null;
  cut_utc: string | null; speed_watermark_utc: string | null; last_speed_window_utc: string | null; windows: Win[]; handoffs: Stair[];
  speed_track: { at: string; watermark: string; batch_id: number }[]; batch_lane: boolean; empty?: string };

export type Handoff = { handoff_id: string; status: string; events: { status: string; at: string }[]; batch_state_prev: string | null;
  batch_state: string | null; snapshot_5min: number | null; snapshot_hourly: number | null; checks: Record<string, any> | null;
  batch_run_id: string | null; message: string | null; started_at: string; finished_at: string; duration_s: number | null;
  published_windows: number | null; speed_windows_compared: number | null; speed_windows_without_batch: number | null };
export type Handoffs = { lane: string; batch_state: string | null; handoffs: Handoff[]; control_table: string | null; batch_lane: boolean };

export type StreamPoint = { ts: string; batch_id: number; num_input_rows: number; input_rows_per_sec: number; processed_rows_per_sec: number;
  trigger_execution_ms: number; add_batch_ms: number | null; commit_offsets_ms: number | null; max_offsets_behind: string | null };
export type Metrics = { lane: string; streaming: Record<string, StreamPoint[]>;
  latency: { ts: string; batch_id: number; n_items: number; p50_s: number; p95_s: number; merge_ms: number }[];
  kafka: { topic: string; rate: [string, number][]; total: [string, number][] } | { error: string };
  dag: { dag_id: string; is_paused: boolean; schedule: string; runs: { run_id: string; state: string; start: string; end: string | null; duration_s: number | null }[];
    tasks: { task_id: string; state: string | null; duration_s: number | null; try_number: number }[] } | { error: string } | null;
  inference: { run_id: string; status: string; reason: string | null; seconds: number; started_at: string; finished_at: string; n_origins?: number; n_predicted?: number }[];
  trino: { at: string; ms: number; ok: boolean; sql: string }[]; not_available: Record<string, string> };

export type FlowNode = { label: string; sub: string; status: "ok" | "degraded" | "error" | "unknown"; metrics: Record<string, any>;
  last_update: string | null; detail: any };
export type Flow = { lane: string; nodes: Record<string, FlowNode>; edges: [string, string][]; generated_at: string };

/** Màu ngữ nghĩa của lớp dữ liệu. Batch và Speed dùng 2 slot đầu của bảng màu series; trạng thái sức khoẻ dùng màu status. */
export const LAYER = { batch: "#2a78d6", batchAfter: "#b9d3f2", speed: "#eb6834", superseded: "#f3c2ad", pending: "#c9ccd1", none: "#eef0ec" };
export const HEALTH: Record<string, { color: string; label: string }> = {
  ok: { color: "#0ca30c", label: "Bình thường" }, degraded: { color: "#fab219", label: "Chậm cập nhật" },
  error: { color: "#d03b3b", label: "Lỗi" }, unknown: { color: "#9aa0a6", label: "Không có số liệu" },
};
export const HANDOFF_STATUS: Record<string, { color: string; label: string }> = {
  PREPARING: { color: "#9aa0a6", label: "Đang chuẩn bị" }, VALIDATED: { color: "#2a78d6", label: "Đã kiểm tra" },
  PUBLISHED: { color: "#0ca30c", label: "Đã công bố" }, FAILED: { color: "#d03b3b", label: "Thất bại" },
};
