import { useEffect, useRef } from "react";
import * as echarts from "echarts/core";
import { BarChart, CustomChart, HeatmapChart, LineChart, ScatterChart } from "echarts/charts";
import {
  DatasetComponent, DataZoomComponent, GridComponent, LegendComponent, MarkAreaComponent, MarkLineComponent, MarkPointComponent,
  TooltipComponent, VisualMapComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { ReactNode } from "react";
import { FORECAST_STATUS, stateMeta } from "../lib";

echarts.use([BarChart, CustomChart, HeatmapChart, LineChart, ScatterChart, DatasetComponent, GridComponent, LegendComponent,
  MarkAreaComponent, MarkLineComponent, MarkPointComponent, DataZoomComponent, TooltipComponent, VisualMapComponent, CanvasRenderer]);

export const INK = { ink: "#1d2329", ink2: "#4b5560", ink3: "#7a838c", rule: "#e3e5e0", axis: "#b9bdb5", signal: "#1f5fbf" };
/** Thứ tự màu series cố định (dataviz reference palette, slot 1..4) - không dùng cho trạng thái giao thông. */
export const SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#6250d6"];

export const baseChart = (): echarts.EChartsCoreOption => ({
  textStyle: { fontFamily: "Public Sans, system-ui, sans-serif", color: INK.ink2, fontSize: 12 },
  grid: { left: 48, right: 16, top: 28, bottom: 32, containLabel: false },
  tooltip: { trigger: "axis", backgroundColor: "#fff", borderColor: INK.axis, textStyle: { color: INK.ink, fontSize: 12 },
    axisPointer: { type: "line", lineStyle: { color: INK.axis } } },
  animationDuration: 300,
});
export const axisStyle = {
  axisLine: { lineStyle: { color: INK.axis } }, axisTick: { show: false },
  axisLabel: { color: INK.ink3, fontSize: 11 }, splitLine: { lineStyle: { color: INK.rule } },
};

export function Chart({ option, height = 260, label }: { option: echarts.EChartsCoreOption; height?: number; label: string }) {
  const el = useRef<HTMLDivElement>(null);
  const inst = useRef<echarts.ECharts | null>(null);
  useEffect(() => {
    if (!el.current) return;
    inst.current = echarts.init(el.current, undefined, { renderer: "canvas" });
    const ro = new ResizeObserver(() => inst.current?.resize());
    ro.observe(el.current);
    return () => { ro.disconnect(); inst.current?.dispose(); inst.current = null; };
  }, []);
  useEffect(() => { inst.current?.setOption(option, true); }, [option]);
  return <div ref={el} role="img" aria-label={label} style={{ width: "100%", height }} />;
}

export function Section({ title, note, children, flush, actions }: {
  title: string; note?: ReactNode; children: ReactNode; flush?: boolean; actions?: ReactNode;
}) {
  return (
    <section className="section">
      <header><h2>{title}</h2>{actions ?? (note ? <span className="note">{note}</span> : null)}</header>
      <div className={flush ? "body flush" : "body"}>{children}</div>
    </section>
  );
}

export function StateLabel({ state, text }: { state?: string | null; text?: string }) {
  const m = stateMeta(state);
  return <span className="state"><i className={m.hatch ? "hatch" : ""} style={{ background: m.color }} />{text ?? m.label}</span>;
}

export function StatusText({ status }: { status: string }) {
  const m = FORECAST_STATUS[status] ?? { label: status, tone: "muted" as const };
  return <span className={m.tone === "muted" ? "muted" : m.tone}>{m.label}</span>;
}

export function Loadable({ loading, error, empty, children }: {
  loading?: boolean; error?: string | null; empty?: ReactNode | false; children: ReactNode;
}) {
  if (error) return <div className="error"><b>Không tải được dữ liệu.</b> {error}</div>;
  if (loading) return <div className="loading">Đang truy vấn…</div>;
  if (empty) return <div className="empty">{empty}</div>;
  return <>{children}</>;
}
