import { createContext, useContext, useEffect, useState } from "react";
import { RoadBackdrop, SurveyGrid } from "./components/RoadBackdrop";
import { RailMap } from "./components/RailMap";
import { ago, duration, minutesBetween, nyTime, useData, vnTime, type Lane } from "./lib";
import Overview from "./pages/Overview";
import MapPage from "./pages/MapPage";
import Forecast from "./pages/Forecast";
import ModelEval from "./pages/ModelEval";
import Ops from "./pages/Ops";

export type Meta = {
  lanes: { id: Lane; label: string; available: boolean }[];
  clock: Record<string, { event_clock_utc: string | null; processed_at_utc: string | null; now_utc: string; actual_available_utc: string | null } | { error: string }>;
  lambda: { batch_state?: string; last_status?: string; error?: string };
  lambda_by_lane?: Record<string, { batch_state?: string; cut_utc?: string; last_status?: string; error?: string }>;
  replay: null | Record<string, any>;
  data_source: string;
};

// Biểu tượng nét mảnh 18 px, vẽ tay theo nội dung từng trang (không dùng bộ icon ngoài)
const ICON: Record<string, JSX.Element> = {
  overview: <><path d="M3 15V9M7.5 15V5M12 15v-4M16.5 15V7" /><path d="M2 16.5h15" /></>,
  map: <><path d="M2.5 4.5 7 3l5 1.5 4.5-1.5v12L12 16.5 7 15l-4.5 1.5z" /><path d="M7 3v12M12 4.5v12" /></>,
  forecast: <><circle cx="9" cy="9.5" r="6.5" /><path d="M9 5.5v4l2.8 1.8" /><path d="M14.5 2.5 16.5 4.5" /></>,
  model: <><path d="M2.5 16.5 16.5 2.5" strokeDasharray="2 2" /><circle cx="5" cy="12" r="1.2" /><circle cx="8" cy="10.5" r="1.2" /><circle cx="10" cy="6.5" r="1.2" /><circle cx="13.5" cy="5" r="1.2" /><circle cx="12.5" cy="9" r="1.2" /></>,
  ops: <><rect x="1.8" y="3" width="4.4" height="4" rx="1" /><rect x="11.8" y="3" width="4.4" height="4" rx="1" /><rect x="6.8" y="11" width="4.4" height="4" rx="1" /><path d="M6.2 5h5.6M14 7v2.5H9V11M4 7v2.5h5" /></>,
};

const ROUTES = [
  { path: "overview", label: "Tổng quan", el: Overview },
  { path: "map", label: "Bản đồ", el: MapPage },
  { path: "forecast", label: "Dự báo 15 phút", el: Forecast },
  { path: "model", label: "Đánh giá mô hình", el: ModelEval },
  { path: "ops", label: "Vận hành", el: Ops },
];

type Ctx = { lane: Lane; meta: Meta | null; go: (path: string, params?: Record<string, string>) => void; params: URLSearchParams };
const AppCtx = createContext<Ctx>({ lane: "main", meta: null, go: () => {}, params: new URLSearchParams() });
export const useApp = () => useContext(AppCtx);

function readHash() {
  const raw = window.location.hash.replace(/^#\/?/, "");
  const [path, qs] = raw.split("?");
  return { path: ROUTES.some((r) => r.path === path) ? path : "overview", params: new URLSearchParams(qs ?? "") };
}

export default function App() {
  const [route, setRoute] = useState(readHash);
  const [lane, setLane] = useState<Lane>(() => {
    // ?lane=demo trên URL (chia sẻ link, chụp màn hình) ưu tiên hơn lựa chọn đã lưu
    const fromUrl = readHash().params.get("lane");
    if (fromUrl === "main" || fromUrl === "demo") return fromUrl;
    try { return (localStorage.getItem("trafficsp.lane") as Lane) || "main"; } catch { return "main"; }
  });
  const { data: meta } = useData<Meta>("/api/meta", 60000);

  useEffect(() => {
    const on = () => setRoute(readHash());
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  useEffect(() => {
    // làn demo biến mất (phiên đã dừng) -> quay về hệ thống chính
    if (meta && lane === "demo" && !meta.lanes.find((l) => l.id === "demo")?.available) setLane("main");
  }, [meta, lane]);
  useEffect(() => { try { localStorage.setItem("trafficsp.lane", lane); } catch { /* bỏ qua */ } }, [lane]);

  const go = (path: string, params?: Record<string, string>) => {
    window.location.hash = `/${path}${params ? `?${new URLSearchParams(params)}` : ""}`;
  };
  const Page = ROUTES.find((r) => r.path === route.path)!.el;

  return (
    <AppCtx.Provider value={{ lane, meta, go, params: route.params }}>
      <div className="shell">
        <SurveyGrid />
        {route.path !== "map" && <RoadBackdrop />}
        <aside className="rail">
          <div className="brand">
            <div className="brand-name">TrafficSP</div>
            <div className="brand-sub">124 đoạn đường có cảm biến, New York</div>
          </div>
          <nav className="nav" aria-label="Trang">
            {ROUTES.map((r) => (
              <a key={r.path} href={`#/${r.path}`} aria-current={r.path === route.path ? "page" : undefined}>
                <svg className="nav-icon" viewBox="0 0 19 19" aria-hidden="true">{ICON[r.path]}</svg>{r.label}
              </a>
            ))}
          </nav>
          {route.path !== "map" && <RailMap lane={lane} onOpen={() => go("map")} />}
          <div className="rail-foot">
            <fieldset className="lane-switch" style={{ border: 0, padding: 0, margin: "0 0 12px" }}>
              <legend style={{ padding: 0, marginBottom: 6, color: "#4b5560" }}>Nguồn hiển thị</legend>
              {(meta?.lanes ?? [{ id: "main", label: "Hệ thống chính", available: true }]).map((l) => (
                <label key={l.id} title={l.available ? "" : "Chưa có phiên phát lại kiểm chứng"}>
                  <input type="radio" name="lane" value={l.id} checked={lane === l.id} disabled={!l.available}
                    onChange={() => setLane(l.id)} />
                  <span>{l.label}</span>
                </label>
              ))}
            </fieldset>
            Dữ liệu NYC DOT và Open-Meteo năm 2025, phát lại qua Kafka.
          </div>
        </aside>
        <div className="main">
          <ClockStrip meta={meta} lane={lane} />
          <main className="page" key={`${route.path}-${lane}`}><Page /></main>
        </div>
      </div>
    </AppCtx.Provider>
  );
}

function ClockStrip({ meta, lane }: { meta: Meta | null; lane: Lane }) {
  const c = meta?.clock?.[lane];
  const clk = c && !("error" in c) ? c : null;
  const replay = meta?.replay;
  const replayActive = replay && replay.state === "running";
  const lag = clk ? minutesBetween(clk.actual_available_utc, clk.event_clock_utc) : null;
  const lam: { batch_state?: string; cut_utc?: string } | undefined = meta?.lambda_by_lane?.[lane] ?? (lane === "main" ? meta?.lambda : undefined);
  return (
    <div className="clock" role="status" aria-label="Đồng hồ dữ liệu">
      <div className="event">
        <div className="k">Giờ sự kiện mới nhất, giờ New York</div>
        <div className="v">{clk ? nyTime(clk.event_clock_utc) : "—"}</div>
        <div className="sub">Thời điểm cảm biến đo, không phải giờ hiện tại</div>
      </div>
      <div>
        <div className="k">Hệ thống ghi nhận lúc (giờ Việt Nam)</div>
        <div className="v">{clk ? vnTime(clk.processed_at_utc) : "—"}</div>
        <div className="sub">{clk ? `cách đây ${ago(clk.processed_at_utc, clk.now_utc)}` : ""}</div>
      </div>
      <div>
        <div className="k">Cửa sổ 5 phút đầy đủ tới</div>
        <div className="v">{clk ? nyTime(clk.actual_available_utc) : "—"}</div>
        <div className="sub">{lag !== null ? `trễ ${duration(lag)} so với giờ sự kiện` : ""}</div>
      </div>
      <div>
        <div className="k">Batch đã công bố tới (batch_state)</div>
        <div className="v">{lam?.batch_state ? nyTime(lam.batch_state) : "—"}</div>
        <div className="sub">{lane === "demo" ? (lam?.cut_utc && lam.batch_state === lam.cut_utc ? "phiên chưa công bố, đang ở mốc bắt đầu" : "batch của phiên")
          : meta?.lambda?.last_status ? `handoff ${meta.lambda.last_status.toLowerCase()}` : ""}</div>
      </div>
      <div className="replay-flag">
        <span className="tag">{lane === "demo" ? "Phiên phát lại kiểm chứng" : "Phát lại dữ liệu lịch sử"}</span>
        <span className="detail">
          {replayActive
            ? `Đang phát ${replay.run_id}: ${nyTime(replay.event_time_utc)}`
            : replay?.state === "finished" ? `Phiên ${replay.run_id} đã phát xong`
            : replay?.state === "stopped" ? `Phiên ${replay.run_id} đã dừng, dữ liệu được giữ`
            : replay?.state === "failed" ? `Phiên ${replay.run_id} lỗi` : "Không có phiên phát lại đang chạy"}
        </span>
      </div>
    </div>
  );
}
