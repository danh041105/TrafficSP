// Nhúng dashboard Grafana (Kafka, Spark) vào trang Vận hành. Grafana vẽ từ Prometheus; trang này chỉ chọn dashboard,
// truyền topic của làn và khoảng thời gian. Grafana không sẵn sàng thì ghi rõ lý do, không hiện khung trống.
import { useState } from "react";
import { Loadable } from "../../components/ui";
import { useData } from "../../lib";

type G = {
  available: boolean; reason: string | null; public_url: string; prometheus_url: string; topic: string;
  dashboards: { uid: string; title: string; path: string; vars: Record<string, string>; scope: string }[];
};

const RANGES = [{ v: "now-1h", label: "1 giờ" }, { v: "now-6h", label: "6 giờ" }, { v: "now-24h", label: "24 giờ" }];

export function GrafanaPanel({ lane }: { lane: string }) {
  const g = useData<G>(`/api/ops/grafana?lane=${lane}`, 0);
  const [uid, setUid] = useState<string | null>(null);
  const [from, setFrom] = useState("now-1h");
  const d = g.data?.dashboards.find((x) => x.uid === uid) ?? g.data?.dashboards[0];

  const src = (kiosk: boolean) => {
    if (!g.data || !d) return "";
    const q = new URLSearchParams({ orgId: "1", theme: "light", refresh: "30s", from, to: "now" });
    Object.entries(d.vars).forEach(([k, v]) => q.set(`var-${k}`, v));
    return `${g.data.public_url}${d.path}?${q}${kiosk ? "&kiosk" : ""}`;
  };

  return (
    <Loadable loading={g.loading && !g.data} error={g.error}>
      {g.data && !g.data.available && (
        <div className="empty">Không nhúng được Grafana: {g.data.reason}. Kiểm tra container traffic-grafana và các biến
          GF_SECURITY_ALLOW_EMBEDDING, GF_AUTH_ANONYMOUS_ENABLED trong docker-compose.yml.</div>
      )}
      {g.data?.available && d && (
        <>
          <div className="controls grafana-bar">
            <div className="seg" role="group" aria-label="Dashboard Grafana">
              {g.data.dashboards.map((x) => (
                <button key={x.uid} aria-pressed={x.uid === d.uid} onClick={() => setUid(x.uid)}>{x.title}</button>
              ))}
            </div>
            <div className="seg" role="group" aria-label="Khoảng thời gian">
              {RANGES.map((r) => <button key={r.v} aria-pressed={r.v === from} onClick={() => setFrom(r.v)}>{r.label}</button>)}
            </div>
            <span className="muted">{d.scope}</span>
            <span className="grafana-links">
              <a href={src(false)} target="_blank" rel="noopener noreferrer">Mở trong Grafana</a>
              <a href={`${g.data.prometheus_url}/targets`} target="_blank" rel="noopener noreferrer">Prometheus targets</a>
            </span>
          </div>
          <iframe key={`${d.uid}-${from}-${lane}`} className="grafana-frame" src={src(true)} title={`Grafana: ${d.title}`} loading="lazy" />
          <p className="caption">Bảng do Grafana vẽ từ Prometheus (kafka-exporter, Spark PrometheusServlet), tự làm mới mỗi 30 giây;
            trục thời gian là giờ xử lý theo múi giờ trình duyệt. Không có ô lag của consumer group vì Spark tự quản offset trong checkpoint,
            không báo về Kafka; số message chưa đọc xem ở biểu đồ "Offset Kafka chưa đọc" phía trên. Grafana mở xem ẩn danh, chỉ đọc;
            muốn sửa dashboard thì mở trong Grafana và đăng nhập.</p>
        </>
      )}
    </Loadable>
  );
}
