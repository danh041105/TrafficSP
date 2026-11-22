"""Số đo Prometheus cho Serving API (endpoint /metrics, Prometheus kéo 15 giây/lần).

Hai nhóm số đo:
  * Kỹ thuật (RED): số request, độ trễ, lỗi theo từng endpoint  -> API có khỏe không?
  * Nghiệp vụ / độ tươi dữ liệu: số đoạn theo trạng thái, đồng hồ dữ liệu, bao lâu rồi đồng hồ chưa nhích
    -> dữ liệu người dùng đang thấy có mới không, giao thông đang thế nào?
"""
import time

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily, REGISTRY

REQUESTS = Counter("serving_http_requests_total", "Số request HTTP", ["method", "route", "status"])
LATENCY = Histogram("serving_http_request_duration_seconds", "Thời gian xử lý request", ["route"],
                    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30))
SOURCE_ERRORS = Counter("serving_source_errors_total", "Lỗi khi đọc nguồn dữ liệu (Trino/ClickHouse)", ["source"])


class ServingCollector:
    """Tính số đo nghiệp vụ ngay lúc Prometheus kéo (dùng lại cache 5 giây của API, không query thêm)."""

    def __init__(self, source_name, latest_fn, clock_fn):
        self.source_name = source_name
        self.latest_fn = latest_fn
        self.clock_fn = clock_fn
        self._last_clock = None
        self._last_change = time.time()

    def collect(self):
        info = GaugeMetricFamily("serving_source_info", "Nguồn dữ liệu đang dùng", labels=["source"])
        info.add_metric([self.source_name], 1)
        yield info

        up = GaugeMetricFamily("serving_source_up", "1 nếu đọc được nguồn dữ liệu")
        try:
            rows = self.latest_fn()
            clock = self.clock_fn()
            up.add_metric([], 1)
        except Exception:  # noqa: BLE001 - /metrics không được chết theo nguồn dữ liệu
            SOURCE_ERRORS.labels(self.source_name).inc()
            up.add_metric([], 0)
            yield up
            return
        yield up

        states = GaugeMetricFamily("serving_links_by_state", "Số đoạn đường theo trạng thái giao thông",
                                   labels=["traffic_state"])
        counts: dict[str, int] = {}
        for r in rows:
            counts[r["traffic_state"]] = counts.get(r["traffic_state"], 0) + 1
        for s in ("FREE_FLOW", "SLOW", "CONGESTED", "STALE", "NO_VALID_DATA", "NO_BASELINE", "NO_DATA"):
            states.add_metric([s], counts.get(s, 0))
        yield states

        speeds = [r["speed_mph"] for r in rows
                  if r["traffic_state"] in ("FREE_FLOW", "SLOW", "CONGESTED") and r["speed_mph"] is not None]
        avg = GaugeMetricFamily("serving_network_avg_speed_mph", "Tốc độ TB các đoạn có số đo")
        avg.add_metric([], sum(speeds) / len(speeds) if speeds else float("nan"))
        yield avg

        if clock is not None:
            ts = clock.timestamp()
            if ts != self._last_clock:
                self._last_clock, self._last_change = ts, time.time()
            c = GaugeMetricFamily("serving_data_clock_seconds", "Đồng hồ dữ liệu (event time mới nhất, unix giây)")
            c.add_metric([], ts)
            yield c
        stall = GaugeMetricFamily("serving_data_stall_seconds",
                                  "Số giây (giờ thật) kể từ lần cuối đồng hồ dữ liệu nhích lên")
        stall.add_metric([], time.time() - self._last_change)
        yield stall


def install(app: FastAPI, source_name: str, latest_fn, clock_fn):
    REGISTRY.register(ServingCollector(source_name, latest_fn, clock_fn))

    @app.middleware("http")
    async def measure(request: Request, call_next):
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            path = getattr(route, "path", "unmatched")     # /api/links/{link_id}, không bùng nhãn theo id
            if path != "/metrics":
                REQUESTS.labels(request.method, path, str(status)).inc()
                LATENCY.labels(path).observe(time.perf_counter() - start)

    @app.get("/metrics", include_in_schema=False)
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
