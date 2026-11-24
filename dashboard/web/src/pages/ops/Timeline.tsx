// Biểu đồ trung tâm: cửa sổ 5 phút theo GIỜ SỰ KIỆN (Batch đã công bố / Speed / lớp đang phục vụ) và bậc thang batch_state
// theo GIỜ XỬ LÝ. Mọi trạng thái lấy từ API (bảng Iceberg, view phục vụ, lambda_control) - không suy từ đồng hồ máy.
import { useMemo } from "react";
import { axisStyle, baseChart, Chart, INK } from "../../components/ui";
import { nyHour, nyTime, vnTime } from "../../lib";
import { HANDOFF_STATUS, LAYER, type Timeline as TL } from "./types";

const ROWS = ["Batch đã công bố", "Speed", "Lớp đang phục vụ"];

export function WindowTimeline({ data }: { data: TL }) {
  const option = useMemo(() => {
    const w = data.windows;
    const t0 = new Date(w[0]?.t ?? 0).getTime(), t1 = new Date(w[w.length - 1]?.t ?? 0).getTime() + 300000;
    const idx = (iso: string | null) => {          // chỉ trả chỉ số khi mốc nằm trong khoảng hiển thị
      if (!iso) return null;
      const t = new Date(iso).getTime();
      if (t < t0 || t > t1) return null;
      const i = w.findIndex((x) => new Date(x.t).getTime() >= t);
      return i < 0 ? w.length : i;
    };
    const tb = data.batch_state ? new Date(data.batch_state).getTime() : null;
    const maxN = Math.max(1, ...w.map((x) => Math.max(x.batch ?? 0, x.speed ?? 0)));
    const cells: any[] = [];
    w.forEach((x, i) => {
      // Batch đã tính trong snapshot "published" nhưng nằm sau batch_state: view phục vụ chưa dùng, tô nhạt
      const afterT = tb !== null && new Date(x.t).getTime() >= tb;
      if (x.batch) cells.push({ value: [i, 0], color: afterT ? LAYER.batchAfter : LAYER.batch, alpha: afterT ? 1 : 0.35 + 0.65 * (x.batch / maxN), win: x, row: 0, afterT });
      if (x.speed) cells.push({ value: [i, 1], color: x.superseded ? LAYER.superseded : LAYER.speed, alpha: x.superseded ? 1 : 0.35 + 0.65 * (x.speed / maxN), win: x, row: 1 });
      else if (x.pending) cells.push({ value: [i, 1], color: LAYER.pending, alpha: 1, win: x, row: 1, hatch: true });
      const sv = x.serving_batch ? LAYER.batch : x.serving_speed ? LAYER.speed : x.pending ? LAYER.pending : null;
      if (sv) cells.push({ value: [i, 2], color: sv, alpha: 1, win: x, row: 2, hatch: !x.serving_batch && !x.serving_speed });
    });
    const lines: any[] = [];
    const add = (iso: string | null, name: string, style: any) => {
      const i = idx(iso);
      if (i !== null) lines.push({ xAxis: i - 0.5, name, lineStyle: style,
        label: { formatter: name, position: "start", rotate: 0, color: style.color, fontSize: 11, fontWeight: 600, distance: 4 } });
    };
    // so theo mili-giây: chuỗi ISO từ lambda_control và từ view có thể khác độ chính xác phần giây
    const prevs = data.handoffs.filter((h) => h.status === "PUBLISHED" && h.batch_state && new Date(h.batch_state).getTime() !== tb);
    const seen = new Set<number>();
    prevs.forEach((h) => {
      const ms = new Date(h.batch_state as string).getTime();
      if (seen.has(ms)) return;
      seen.add(ms);
      add(h.batch_state, `batch_state cũ (công bố ${vnTime(h.at, false)})`, { color: INK.ink3, type: "dashed", width: 1 });
    });
    if (data.cut_utc && new Date(data.cut_utc).getTime() !== tb) add(data.cut_utc, "đầu phiên", { color: INK.ink3, type: [2, 3], width: 1 });
    add(data.batch_state, "batch_state", { color: INK.ink, type: "solid", width: 2 });
    add(data.event_clock_utc, "sự kiện mới nhất", { color: INK.signal, type: "dotted", width: 2 });
    return {
      ...baseChart(),
      grid: { left: 128, right: 60, top: 36, bottom: 30 },
      tooltip: {
        trigger: "item", backgroundColor: "#fff", borderColor: INK.axis, textStyle: { color: INK.ink, fontSize: 12 },
        formatter: (p: any) => {
          const x = p.data.win as TL["windows"][number];
          const serving = x.serving_batch ? `Batch (${x.serving_batch} đoạn)` : x.serving_speed ? `Speed (${x.serving_speed} đoạn)` : "chưa có cửa sổ";
          return `<b>${nyTime(x.t)}</b> (giờ New York)<br/>Batch đã công bố: ${x.batch ?? "—"} đoạn`
            + (p.data.afterT ? " (sau batch_state, view phục vụ chưa dùng)" : "") + `<br/>Speed: ${x.speed ?? "—"} đoạn`
            + (x.superseded ? " (đã được Batch thay)" : "") + `<br/>Đang phục vụ: ${serving}` + (x.pending ? "<br/>Cửa sổ chưa hoàn tất (sau watermark)" : "");
        },
      },
      xAxis: { type: "value", min: -0.5, max: w.length - 0.5, interval: 6, ...axisStyle, splitLine: { show: false },
        axisLabel: { ...axisStyle.axisLabel, formatter: (v: number) => (w[Math.round(v)] ? nyHour(w[Math.round(v)].t) : "") } },
      yAxis: { type: "category", data: ROWS, inverse: true, ...axisStyle, splitLine: { show: false }, axisLabel: { color: INK.ink2, fontSize: 12 } },
      dataZoom: [{ type: "inside", xAxisIndex: 0 }],
      series: [{
        type: "custom", data: cells, encode: { x: 0, y: 1 },
        renderItem: (params: any, api: any) => {
          const d = cells[params.dataIndex];
          const [x, y] = api.coord([d.value[0], d.value[1]]);
          const bw = api.size([1, 0])[0], bh = api.size([0, 1])[1];
          return { type: "rect", shape: { x: x - bw / 2 + 0.5, y: y - bh * 0.36, width: Math.max(1, bw - 1), height: bh * 0.72 },
            style: { fill: d.color, opacity: d.alpha } };
        },
        markLine: { symbol: "none", silent: true, data: lines },
      }],
    };
  }, [data]);
  return <Chart option={option} height={230} label="Cửa sổ 5 phút theo giờ sự kiện: Batch đã công bố, Speed, lớp đang phục vụ" />;
}

export function BatchStairs({ data }: { data: TL }) {
  const option = useMemo(() => {
    const pub = data.handoffs.filter((h) => h.status === "PUBLISHED" && h.batch_state).sort((a, b) => a.at.localeCompare(b.at));
    const failed = data.handoffs.filter((h) => h.status === "FAILED" && h.batch_state);
    const start = data.speed_track[0]?.at;
    const stairs: [string, string][] = [];
    if (data.cut_utc && start && (!pub.length || start < pub[0].at)) stairs.push([start, data.cut_utc]);
    pub.forEach((h) => stairs.push([h.at, h.batch_state as string]));
    const lastAt = [...data.speed_track.map((s) => s.at), ...pub.map((h) => h.at)].sort().at(-1);
    if (stairs.length && lastAt && lastAt > stairs[stairs.length - 1][0]) stairs.push([lastAt, stairs[stairs.length - 1][1]]);
    const toMs = (s: string) => new Date(s).getTime();
    const ys = [...data.speed_track.map((s) => toMs(s.watermark)), ...stairs.map(([, b]) => toMs(b))];
    const multiDay = ys.length > 1 && Math.max(...ys) - Math.min(...ys) > 86400000;
    return {
      ...baseChart(),
      legend: { top: 0, left: 0, icon: "rect", itemWidth: 12, itemHeight: 3 },
      grid: { left: 96, right: 18, top: 34, bottom: 36 },
      tooltip: { trigger: "axis", backgroundColor: "#fff", borderColor: INK.axis, textStyle: { color: INK.ink, fontSize: 12 },
        formatter: (ps: any[]) => `<b>${vnTime(new Date(ps[0].value[0]).toISOString())}</b> (giờ xử lý, VN)<br/>` +
          ps.map((p) => `${p.marker}${p.seriesName}: ${nyTime(new Date(p.value[1]).toISOString())} NY`).join("<br/>") },
      xAxis: { type: "time", ...axisStyle, axisLabel: { ...axisStyle.axisLabel, formatter: (v: number) => vnTime(new Date(v).toISOString(), false) } },
      yAxis: { type: "time", scale: true, ...axisStyle, axisLabel: { ...axisStyle.axisLabel,
        formatter: (v: number) => (multiDay ? nyTime(new Date(v).toISOString()).replace(/\/\d{4}$/, "") : nyHour(new Date(v).toISOString())) } },
      series: [
        { name: "Speed đã xử lý tới (watermark)", type: "line", step: false, showSymbol: false, lineStyle: { width: 2, color: LAYER.speed },
          itemStyle: { color: LAYER.speed }, data: data.speed_track.map((s) => [toMs(s.at), toMs(s.watermark)]) },
        { name: "batch_state đã công bố", type: "line", step: "end", showSymbol: true, symbolSize: 7, lineStyle: { width: 2, color: LAYER.batch },
          itemStyle: { color: LAYER.batch, borderColor: "#fff", borderWidth: 1.5 }, data: stairs.map(([a, b]) => [toMs(a), toMs(b)]) },
        { name: "Handoff thất bại", type: "scatter", symbol: "diamond", symbolSize: 10, itemStyle: { color: HANDOFF_STATUS.FAILED.color },
          data: failed.map((h) => [toMs(h.at), toMs(h.batch_state as string)]) },
      ],
    };
  }, [data]);
  if (!data.speed_track.length && !data.handoffs.length) return <div className="empty">Chưa có tiến độ Speed Layer hay handoff nào cho làn này.</div>;
  return <Chart option={option} height={230} label="batch_state đã công bố và watermark của Speed Layer theo giờ xử lý" />;
}
