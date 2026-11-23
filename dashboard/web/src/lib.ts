// Tiện ích dùng chung: gọi API, định dạng thời gian/số, màu & nhãn trạng thái giao thông.
import { useCallback, useEffect, useRef, useState } from "react";

export type Lane = "main" | "demo";

export class ApiError extends Error {}

export async function api<T>(path: string): Promise<T> {
  const r = await fetch(path, { headers: { Accept: "application/json" } });
  if (!r.ok) {
    let msg = `${r.status} ${r.statusText}`;
    try { const j = await r.json(); if (j.detail) msg = typeof j.detail === "string" ? j.detail : JSON.stringify(j.detail); } catch { /* giữ msg */ }
    throw new ApiError(msg);
  }
  return r.json() as Promise<T>;
}

/** Tải dữ liệu, tự làm mới theo chu kỳ (ms). Giữ dữ liệu cũ khi lần làm mới lỗi. */
export function useData<T>(path: string | null, refreshMs = 0) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const seq = useRef(0);
  const load = useCallback(async () => {
    if (!path) return;
    const id = ++seq.current;
    setLoading(true);
    try {
      const d = await api<T>(path);
      if (id === seq.current) { setData(d); setError(null); }
    } catch (e) {
      if (id === seq.current) setError(e instanceof Error ? e.message : String(e));
    } finally {
      if (id === seq.current) setLoading(false);
    }
  }, [path]);
  useEffect(() => {
    setData(null); setError(null);
    load();
    if (!refreshMs) return;
    const t = setInterval(load, refreshMs);
    return () => clearInterval(t);
  }, [load, refreshMs]);
  return { data, error, loading, reload: load };
}

// ---------------------------------------------------------------- thời gian
const NY = "America/New_York";
const VN = "Asia/Ho_Chi_Minh";
const fmt = (tz: string, withDate = true, withSec = false) =>
  new Intl.DateTimeFormat("vi-VN", {
    timeZone: tz, hour12: false, hour: "2-digit", minute: "2-digit", ...(withSec ? { second: "2-digit" } : {}),
    ...(withDate ? { day: "2-digit", month: "2-digit", year: "numeric" } : {}),
  });
const parse = (iso?: string | null) => (iso ? new Date(iso) : null);

/** Giờ sự kiện (dữ liệu NYC) hiển thị theo giờ New York. */
export const nyTime = (iso?: string | null, withDate = true) => { const d = parse(iso); return d ? fmt(NY, withDate).format(d) : "—"; };
/** Giờ xử lý (máy chạy hệ thống) hiển thị theo giờ Việt Nam. */
export const vnTime = (iso?: string | null, withDate = true) => { const d = parse(iso); return d ? fmt(VN, withDate).format(d) : "—"; };
export const nyHour = (iso: string) => fmt(NY, false).format(new Date(iso));

export function ago(iso?: string | null, nowIso?: string | null): string {
  const d = parse(iso); if (!d) return "—";
  const now = nowIso ? new Date(nowIso) : new Date();
  const min = Math.round((now.getTime() - d.getTime()) / 60000);
  return duration(min);
}
export function duration(min: number | null | undefined): string {
  if (min === null || min === undefined || Number.isNaN(min)) return "—";
  if (min < 1) return "dưới 1 phút";
  if (min < 60) return `${min} phút`;
  const h = Math.floor(min / 60);
  if (h < 48) return `${h} giờ ${min % 60 ? `${min % 60} phút` : ""}`.trim();
  return `${Math.floor(h / 24)} ngày ${h % 24} giờ`;
}
export const minutesBetween = (a?: string | null, b?: string | null) =>
  a && b ? Math.round((new Date(b).getTime() - new Date(a).getTime()) / 60000) : null;

// ---------------------------------------------------------------- số
export const num = (v: number | null | undefined, d = 1) =>
  v === null || v === undefined || Number.isNaN(v) ? "—" : v.toLocaleString("vi-VN", { minimumFractionDigits: d, maximumFractionDigits: d });
export const int = (v: number | null | undefined) =>
  v === null || v === undefined ? "—" : Math.round(v).toLocaleString("vi-VN");
export const pct = (v: number | null | undefined, d = 1) => (v === null || v === undefined ? "—" : `${num(v * 100, d)}%`);

// ---------------------------------------------------------------- trạng thái giao thông
export const STATES = ["FREE_FLOW", "SLOW", "CONGESTED", "STALE", "NO_VALID_DATA", "NO_DATA", "NO_BASELINE"] as const;
export const STATE_META: Record<string, { label: string; color: string; hatch?: boolean; order: number }> = {
  FREE_FLOW: { label: "Thông thoáng", color: "#0ca30c", order: 0 },
  SLOW: { label: "Chậm", color: "#fab219", order: 1 },
  CONGESTED: { label: "Tắc", color: "#d03b3b", order: 2 },
  STALE: { label: "Số liệu cũ quá 15 phút", color: "#b6bbc0", order: 3 },
  NO_VALID_DATA: { label: "Không có số hợp lệ", color: "#9aa0a6", hatch: true, order: 4 },
  NO_DATA: { label: "Chưa từng có dữ liệu", color: "#9aa0a6", hatch: true, order: 5 },
  NO_BASELINE: { label: "Chưa có tốc độ thông thoáng", color: "#9aa0a6", hatch: true, order: 6 },
  NO_RECENT_DATA: { label: "Không đủ quan sát 60 phút", color: "#9aa0a6", hatch: true, order: 7 },
};
export const stateMeta = (s?: string | null) => STATE_META[s ?? ""] ?? { label: s ?? "—", color: "#9aa0a6", hatch: true, order: 9 };
export const VALID_STATES = new Set(["FREE_FLOW", "SLOW", "CONGESTED"]);

export const FORECAST_STATUS: Record<string, { label: string; tone: "ok" | "warn" | "bad" | "muted" }> = {
  EVALUATED: { label: "Đã đối chiếu", tone: "ok" },
  PENDING: { label: "Chờ tới giờ đích", tone: "muted" },
  BEYOND_DATA: { label: "Sau cuối dữ liệu", tone: "muted" },
  MISSING_ACTUAL: { label: "Thiếu số thực tế", tone: "warn" },
  NO_FORECAST: { label: "Không dự báo", tone: "muted" },
};

export const BOROUGH_ORDER = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"];
