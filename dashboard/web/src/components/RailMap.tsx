// Bản đồ thu nhỏ ở thanh bên: 124 đoạn đường tô theo trạng thái giao thông hiện tại của làn đang chọn.
// Dữ liệu thật từ /api/links (cùng nguồn trang Bản đồ), làm mới mỗi phút. Màu trạng thái luôn kèm chú giải chữ và số lượng.
import { useMemo } from "react";
import { inBox } from "./RoadBackdrop";
import { int, nyTime, stateMeta, useData, VALID_STATES } from "../lib";

type Links = {
  features: { id: number; properties: { traffic_state: string | null; link_name: string }; geometry: { coordinates: [number, number][] } }[];
  clock: { event_clock_utc?: string } | null;
};

const K = Math.cos((40.7 * Math.PI) / 180);

export function RailMap({ lane, onOpen }: { lane: string; onOpen: () => void }) {
  const { data } = useData<Links>(`/api/links?lane=${lane}`, 60000);

  const g = useMemo(() => {
    if (!data?.features?.length) return null;
    // khung vừa khít các điểm hợp lệ để hình lấp đầy bề rộng thanh bên
    const pts = data.features.flatMap((f) => f.geometry.coordinates.filter(inBox));
    if (!pts.length) return null;
    const x0 = Math.min(...pts.map((p) => p[0])), x1 = Math.max(...pts.map((p) => p[0]));
    const y0 = Math.min(...pts.map((p) => p[1])), y1 = Math.max(...pts.map((p) => p[1]));
    const w = (x1 - x0) * K * 1000, h = (y1 - y0) * 1000;
    const xy = (p: [number, number]) => `${((p[0] - x0) * K * 1000).toFixed(1)},${((y1 - p[1]) * 1000).toFixed(1)}`;
    const counts: Record<string, number> = { FREE_FLOW: 0, SLOW: 0, CONGESTED: 0, OTHER: 0 };
    const lines = data.features.map((f) => {
      const st = f.properties.traffic_state ?? "NO_DATA";
      counts[VALID_STATES.has(st) ? st : "OTHER"] += 1;
      const runs: string[] = [];
      let run: [number, number][] = [];
      const flush = () => { if (run.length > 1) runs.push("M" + run.map(xy).join("L")); run = []; };
      f.geometry.coordinates.forEach((p) => (inBox(p) ? run.push(p) : flush()));
      flush();
      return { id: f.id, d: runs.join(""), st, valid: VALID_STATES.has(st), name: f.properties.link_name };
    });
    // vẽ nét không có số liệu trước để nét có màu nằm trên
    lines.sort((a, b) => Number(a.valid) - Number(b.valid));
    return { w, h, lines, counts };
  }, [data]);

  if (!g) return <div className="railmap railmap-empty" />;
  const pad = 14;
  return (
    <figure className="railmap">
      <button type="button" className="railmap-btn" onClick={onOpen} aria-label="Mở trang Bản đồ: trạng thái 124 đoạn đường">
        <svg viewBox={`${-pad} ${-pad} ${(g.w + 2 * pad).toFixed(0)} ${(g.h + 2 * pad).toFixed(0)}`} preserveAspectRatio="xMidYMid meet">
          {g.lines.map((l) => (
            <path key={l.id} d={l.d} fill="none" stroke={l.valid ? stateMeta(l.st).color : "#b6bbc0"}
              strokeWidth={l.valid ? 2.4 : 1.4} strokeDasharray={l.valid ? undefined : "3 3"}
              strokeLinecap="round" strokeLinejoin="round" vectorEffect="non-scaling-stroke">
              <title>{`${l.name}: ${stateMeta(l.st).label}`}</title>
            </path>
          ))}
        </svg>
      </button>
      <figcaption>
        <div className="railmap-title">Mạng lưới lúc {nyTime(data?.clock?.event_clock_utc)}</div>
        <ul className="railmap-legend">
          {(["FREE_FLOW", "SLOW", "CONGESTED"] as const).map((s) => (
            <li key={s}><i style={{ background: stateMeta(s).color }} />{stateMeta(s).label} <b>{int(g.counts[s])}</b></li>
          ))}
          <li><i className="dash" />Không có số liệu hợp lệ <b>{int(g.counts.OTHER)}</b></li>
        </ul>
      </figcaption>
    </figure>
  );
}
