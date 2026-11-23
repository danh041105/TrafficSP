// Hình nền: mạng 124 đoạn đường có cảm biến, vẽ từ hình học thật (serving.v_link_dim), trên lưới giấy khảo sát.
// Chỉ là lớp nền mờ (aria-hidden, không nhận chuột); không mang số liệu nào nên không có chú giải màu.
import { useEffect, useState } from "react";

type Geo = { links: { id: number; borough: string; c: [number, number][] }[]; n_links: number };

// Khung toạ độ New York City. Điểm nằm ngoài là lỗi hình học của nguồn: bỏ và ngắt nét, không nối thẳng qua.
export const BOX = { lon0: -74.27, lon1: -73.68, lat0: 40.48, lat1: 40.93 };
export const inBox = ([x, y]: [number, number]) => x >= BOX.lon0 && x <= BOX.lon1 && y >= BOX.lat0 && y <= BOX.lat1;
const K = Math.cos((40.7 * Math.PI) / 180);      // co kinh độ theo vĩ độ để hình không bị bè ngang
export const W = (BOX.lon1 - BOX.lon0) * K * 1000, H = (BOX.lat1 - BOX.lat0) * 1000;
export const px = ([x, y]: [number, number]) => `${((x - BOX.lon0) * K * 1000).toFixed(1)},${((BOX.lat1 - y) * 1000).toFixed(1)}`;

function paths(g: Geo) {
  const lines: string[] = [], ends: string[] = [];
  const acc: Record<string, { x: number; y: number; n: number }> = {};
  g.links.forEach((l) => {
    let run: [number, number][] = [];
    const flush = () => { if (run.length > 1) lines.push("M" + run.map(px).join("L")); run = []; };
    l.c.forEach((p) => {
      if (!inBox(p)) return flush();
      run.push(p);
      const a = (acc[l.borough] ??= { x: 0, y: 0, n: 0 });
      a.x += p[0]; a.y += p[1]; a.n += 1;
    });
    flush();
    const first = l.c.find(inBox);
    if (first) ends.push(px(first));
  });
  // nhãn quận đặt ở trọng tâm các điểm đo của quận đó (tính từ dữ liệu, không đặt tay)
  const labels = Object.entries(acc).filter(([b]) => b && b !== "null")
    .map(([b, a]) => ({ b, xy: px([a.x / a.n, a.y / a.n]).split(",").map(Number) as [number, number] }))
    .sort((p, q) => p.xy[1] - q.xy[1]);
  // tránh chồng chữ: nhãn gần nhau theo chiều ngang (ước lượng bề rộng theo số ký tự) bị đẩy xuống một dòng
  labels.forEach((l, i) => {
    for (let j = 0; j < i; j++) {
      const o = labels[j], half = (l.b.length + o.b.length) * 9.5;
      if (Math.abs(l.xy[0] - o.xy[0]) < half && Math.abs(l.xy[1] - o.xy[1]) < 34) l.xy = [l.xy[0], o.xy[1] + 34];
    }
  });
  return { d: lines.join(""), ends, labels };
}

/** Lưới giấy khảo sát phủ cả vùng nội dung, cố định khi cuộn. */
export function SurveyGrid() {
  return (
    <svg className="backdrop" aria-hidden="true" width="100%" height="100%">
      <defs>
        <pattern id="bd-fine" width="24" height="24" patternUnits="userSpaceOnUse">
          <path d="M24 0H0V24" fill="none" stroke="var(--grid-fine)" strokeWidth="1" />
        </pattern>
        <pattern id="bd-major" width="120" height="120" patternUnits="userSpaceOnUse">
          <rect width="120" height="120" fill="url(#bd-fine)" />
          <path d="M120 0H0V120" fill="none" stroke="var(--grid-major)" strokeWidth="1" />
        </pattern>
      </defs>
      <rect width="100%" height="100%" fill="url(#bd-major)" />
    </svg>
  );
}

/** Mạng đường cảm biến: lớp nền cố định sau vùng nội dung, nét rất nhạt; tên quận chữ lớn kiểu tờ bản đồ khảo sát. */
export function RoadBackdrop() {
  const [g, setG] = useState<(ReturnType<typeof paths> & { n: number }) | null>(null);
  useEffect(() => {
    let alive = true;
    fetch("/api/geometry").then((r) => (r.ok ? r.json() : null)).then((j: Geo | null) => {
      if (alive && j?.links?.length) setG({ ...paths(j), n: j.n_links });
    }).catch(() => { /* hình nền là phụ: lỗi thì bỏ qua */ });
    return () => { alive = false; };
  }, []);
  if (!g) return null;
  return (
    <figure className="roadnet" aria-hidden="true">
      <svg viewBox={`-40 -10 ${(W + 80).toFixed(0)} ${(H + 20).toFixed(0)}`} preserveAspectRatio="xMaxYMax meet">
        {g.labels.map((l) => <text key={l.b} x={l.xy[0]} y={l.xy[1]} className="roadnet-label" textAnchor="middle">{l.b}</text>)}
        <path d={g.d} fill="none" stroke="var(--road-ink)" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"
          vectorEffect="non-scaling-stroke" />
        {g.ends.map((e, i) => {
          const [x, y] = e.split(",");
          return <circle key={i} cx={x} cy={y} r="2.2" fill="var(--page)" stroke="var(--road-ink)" strokeWidth="1.2" vectorEffect="non-scaling-stroke" />;
        })}
      </svg>
      <figcaption>Nền: {g.n} đoạn đường có cảm biến NYC DOT, vẽ từ toạ độ thật trong dim_road_link</figcaption>
    </figure>
  );
}
