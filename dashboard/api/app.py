"""API của dashboard nghiệp vụ TrafficSP (FastAPI). Chỉ đọc. Mọi số liệu lấy từ nguồn thật:
- Trino: serving.*, gold.lambda_control, ml.*;
- Spark master JSON, Prometheus, Airflow REST v2;
- log vận hành của Speed Layer và suy luận (JSONL);
- artifact đánh giá ML: reports/ml/<phiên bản của mô hình đang phục vụ>/metrics.json;
- trạng thái phiên phát lại: JSON do scripts/demo_replay.py ghi.

Hai "làn" dữ liệu:
- main: hệ thống chính (serving.v_*), batch_state do Lambda handoff công bố.
- demo: phiên phát lại kiểm chứng (serving.v_demo_*), tạo bởi scripts/demo_replay.py, tách khỏi batch_state chính.

An toàn cho Trino:
- Mọi truy vấn trên view hợp nhất đều lọc theo MỐC HẰNG SỐ suy từ đồng hồ sự kiện, để Iceberg cắt được partition/file.
  Lọc theo subquery max(...) buộc quét cả 9 triệu dòng; nhiều bản như vậy chồng lên nhau làm Trino hết heap.
- Cache TTL có single-flight: nhiều yêu cầu cùng khóa chỉ chạy MỘT truy vấn, các yêu cầu khác chờ kết quả đó.
Credential (Airflow) chỉ nằm phía server; frontend không bao giờ nhận.
"""
from __future__ import annotations

import datetime as dt
import glob
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from trino_client import TrinoError
from trino_client import query as _trino_query

# Độ trễ truy vấn Trino do chính backend đo (bộ đệm vòng, mất khi restart container) - nguồn cho biểu đồ "Trino" trang Vận hành
TRINO_TIMINGS: deque = deque(maxlen=600)


def query(sql: str, timeout: int = 60):
    t0 = time.time()
    ok = True
    try:
        return _trino_query(sql, timeout)
    except Exception:
        ok = False
        raise
    finally:
        TRINO_TIMINGS.append({"at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                              "ms": round((time.time() - t0) * 1000), "ok": ok, "sql": " ".join(sql.split())[:70]})

REPORTS_DIR = Path(os.getenv("REPORTS_DIR", "/app/reports/ml"))
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/app/models"))
OPS_LOG_DIR = Path(os.getenv("OPS_LOG_DIR", "/app/opslogs"))
WEB_DIR = Path(os.getenv("WEB_DIR", "/app/web"))
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "http://spark-master:8080")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-apiserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "")
DAGS = [d for d in os.getenv("DASHBOARD_DAGS", "trafficsp_lambda_batch,trafficsp_ml_inference").split(",") if d]
STALE_MIN = int(os.getenv("STALE_AFTER_MINUTES", "15"))

LANES = {
    "main": {"prefix": "v_", "label": "Hệ thống chính"},
    "demo": {"prefix": "v_demo_", "label": "Phiên phát lại kiểm chứng"},
}

app = FastAPI(title="TrafficSP dashboard API", docs_url="/api/docs", openapi_url="/api/openapi.json")

# ---------------------------------------------------------------- cache TTL + single-flight
_cache: dict = {}
_inflight: dict = {}
_lock = threading.Lock()


_force = threading.local()          # luồng làm ấm cache đặt cờ này để tính lại dù cache còn hạn


def cached(key, ttl: float, fn):
    while True:
        with _lock:
            hit = _cache.get(key)
            if hit and time.time() - hit[0] < ttl and not getattr(_force, "on", False):
                return hit[1]
            ev = _inflight.get(key)
            if ev is None:                      # mình là người tính
                ev = _inflight[key] = threading.Event()
                break
        ev.wait(timeout=120)                    # người khác đang tính cùng khóa: chờ rồi đọc cache
        with _lock:
            hit = _cache.get(key)
            if hit:
                return hit[1]
    try:
        val = fn()
        with _lock:
            _cache[key] = (time.time(), val)
        return val
    finally:
        with _lock:
            _inflight.pop(key, None)
        ev.set()


def q(sql: str, ttl: float = 120):
    return cached(("sql", sql), ttl, lambda: query(sql))


_pool = ThreadPoolExecutor(max_workers=4)


def parallel(**fns):
    """Chạy song song các truy vấn độc lập của một endpoint (tối đa 4 luồng)."""
    futs = {k: _pool.submit(f) for k, f in fns.items()}
    return {k: f.result() for k, f in futs.items()}


def view(lane: str, name: str) -> str:
    if lane not in LANES:
        raise HTTPException(400, f"lane không hợp lệ: {lane}")
    return f"lakehouse.serving.{LANES[lane]['prefix']}{name}"


def lane_exists(lane: str) -> bool:
    if lane == "main":
        return True
    rows = q("SELECT count(*) AS n FROM lakehouse.information_schema.tables "
             "WHERE table_schema = 'serving' AND table_name = 'v_demo_traffic_latest'", ttl=150)
    return bool(rows and rows[0]["n"])


def require_lane(lane: str):
    if not lane_exists(lane):
        raise HTTPException(404, "Chưa có phiên phát lại kiểm chứng. Chạy scripts/demo_replay.py start để tạo.")


def http_json(url: str, data: bytes | None = None, headers: dict | None = None, timeout: float = 5):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method="POST" if data else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def safe(fn):
    """Lỗi của một nguồn phụ không làm hỏng cả trang: trả {"error": "..."} cho khối đó."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 - báo nguyên văn lỗi nguồn cho trang Vận hành
        return {"error": f"{type(e).__name__}: {e}"}


def tail_lines(path: Path, n: int) -> list[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        return list(deque(f, maxlen=n))


def read_json(p: Path):
    with p.open(encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------- đồng hồ & mốc hằng số
def parse_ts(s: str | None) -> dt.datetime | None:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def lit(t: dt.datetime) -> str:
    """Hằng số timestamp cho Trino (UTC) - cho phép cắt partition/file thay vì subquery."""
    return f"TIMESTAMP '{t.astimezone(dt.timezone.utc):%Y-%m-%d %H:%M:%S} UTC'"


def clock(lane: str) -> dict:
    latest, unified = view(lane, "traffic_latest"), view(lane, "traffic_5min_unified")

    def run():
        c = query(f"""SELECT to_iso8601(max(event_clock_utc)) AS event_clock_utc,
                             to_iso8601(max(updated_at)) AS processed_at_utc,
                             to_iso8601(current_timestamp) AS now_utc
                      FROM {latest}""")[0]
        t = parse_ts(c["event_clock_utc"])
        c["actual_available_utc"] = None
        if t:
            # Cửa sổ 5' mới nhất đã có trong view hợp nhất = mốc "số thực tế đã sẵn sàng" (Speed phát cửa sổ sau watermark)
            c["actual_available_utc"] = query(f"""SELECT to_iso8601(max(window_end_utc)) AS a FROM {unified}
                WHERE window_start_utc >= {lit(t - dt.timedelta(days=1))} AND window_start_utc <= {lit(t)}""")[0]["a"]
        return c
    return cached(("clock", lane), 120, run)


def event_clock(lane: str) -> dt.datetime:
    t = parse_ts(clock(lane)["event_clock_utc"])
    if t is None:
        raise HTTPException(409, f"Làn {lane} chưa có bản ghi nào trong traffic_latest")
    return t


@app.get("/api/meta")
def meta():
    lanes = [{"id": k, "label": v["label"], "available": safe(lambda k=k: lane_exists(k)) is True} for k, v in LANES.items()]
    clocks = {ln["id"]: safe(lambda i=ln["id"]: clock(i)) for ln in lanes if ln["available"]}
    st = safe(lambda: q("""SELECT to_iso8601(batch_state) AS batch_state, last_status
                           FROM lakehouse.serving.v_lambda_status""", ttl=150)[0])
    lam = {"main": st}
    if any(ln["id"] == "demo" and ln["available"] for ln in lanes):
        lam["demo"] = safe(lambda: q("""SELECT to_iso8601(batch_state) AS batch_state, to_iso8601(cut_utc) AS cut_utc
                                        FROM lakehouse.serving.v_demo_batch_state""", ttl=20)[0])
    return {"lanes": lanes, "clock": clocks, "lambda": st, "lambda_by_lane": lam, "replay": latest_replay_status(), "demo_lane": demo_lane(),
            "data_source": "Dữ liệu cảm biến NYC DOT và thời tiết Open-Meteo năm 2025, phát lại qua Kafka (không phải dữ liệu trực tiếp)"}


# ---------------------------------------------------------------- tổng quan
VALID = "('FREE_FLOW','SLOW','CONGESTED')"


@app.get("/api/overview")
def overview(lane: str = "main", hours: int = Query(6, ge=1, le=48)):
    require_lane(lane)
    latest, enriched = view(lane, "traffic_latest"), view(lane, "traffic_5min_enriched")
    t = event_clock(lane)
    r = parallel(
        states=lambda: q(f"SELECT traffic_state AS state, count(*) AS n FROM {latest} GROUP BY 1 ORDER BY 2 DESC"),
        kpi=lambda: q(f"""SELECT count(*) AS links, count_if(traffic_state IN {VALID}) AS links_valid,
                       avg(CASE WHEN traffic_state IN {VALID} THEN speed_mph END) AS avg_speed_mph,
                       approx_percentile(CASE WHEN traffic_state IN {VALID} THEN valid_age_min END, 0.5) AS median_valid_age_min,
                       max(valid_age_min) FILTER (WHERE traffic_state IN {VALID}) AS max_valid_age_min
                FROM {latest}""")[0],
        borough=lambda: q(f"""SELECT borough, traffic_state AS state, count(*) AS n,
                           avg(CASE WHEN traffic_state IN {VALID} THEN speed_mph END) AS avg_speed_mph
                    FROM {latest} GROUP BY 1, 2 ORDER BY 1, 2"""),
        trend=lambda: q(f"""SELECT to_iso8601(window_start_utc) AS t, traffic_state AS state, count(*) AS n,
                         avg(avg_speed_mph) AS avg_speed_mph
                  FROM {enriched}
                  WHERE window_start_utc >= {lit(t - dt.timedelta(hours=hours))} AND window_start_utc <= {lit(t)}
                    AND n_valid > 0 AND traffic_state IS NOT NULL
                  GROUP BY 1, 2 ORDER BY 1""", ttl=180))
    return {"lane": lane, "clock": clock(lane), **r, "stale_after_min": STALE_MIN, "trend_hours": hours}


# ---------------------------------------------------------------- bản đồ
# Khung NYC (lon, lat) rộng hơn 5 quận một chút; điểm nằm ngoài là toạ độ hỏng của nguồn.
NYC_BOX = (-74.30, 40.45, -73.65, 40.95)


def _km(a, b) -> float:
    """Khoảng cách xấp xỉ (km) ở vĩ độ NYC: 1° kinh ≈ 84,4 km, 1° vĩ ≈ 111 km."""
    return (((a[0] - b[0]) * 84.4) ** 2 + ((a[1] - b[1]) * 111.0) ** 2) ** 0.5


def clean_line(pts: list[list[float]]) -> tuple[list[list[float]], int]:
    """Bỏ điểm toạ độ hỏng trong link_points của NYC DOT (nguồn có điểm bị cắt chữ số, vd -74.91 thay vì -73.99x,
    40.8705 giữa các điểm 40.80x). Silver chỉ làm sạch bản ghi tốc độ, link_points đi thẳng từ nguồn nên phải lọc ở đây.
    1. Bỏ điểm nằm ngoài khung NYC.
    2. Bỏ "gai": điểm giữa mà cả hai cạnh kề đều > 0,5 km và đường vòng qua nó dài hơn 3 lần (+0,5 km) đường nối thẳng
       hai điểm kề. Đường thật đi thưa điểm (cạnh 1-2 km thẳng hàng) không bị ảnh hưởng vì a + b ≈ c.
    Không cắt điểm đầu/cuối chỉ vì cạnh dài: nhiều đoạn cao tốc (WSE, CBE) có điểm cuối thật cách 1-1,5 km.
    Trả về (điểm còn lại, số điểm đã bỏ)."""
    x0, y0, x1, y1 = NYC_BOX
    out = [p for p in pts if x0 < p[0] < x1 and y0 < p[1] < y1]
    changed = True
    while changed and len(out) >= 3:
        changed = False
        for i in range(1, len(out) - 1):
            a, b, c = _km(out[i - 1], out[i]), _km(out[i], out[i + 1]), _km(out[i - 1], out[i + 1])
            if min(a, b) > 0.5 and a + b > 3 * c + 0.5:
                del out[i]
                changed = True
                break
    return out, len(pts) - len(out)


def wkt_to_coords(wkt: str | None, with_dropped: bool = False):
    if not wkt or not wkt.upper().startswith("LINESTRING"):
        return (None, 0) if with_dropped else None
    body = wkt[wkt.index("(") + 1: wkt.rindex(")")]
    pts = []
    for p in body.split(","):
        xy = p.strip().split()
        if len(xy) >= 2:
            pts.append([float(xy[0]), float(xy[1])])
    pts, dropped = clean_line(pts)
    pts = pts if len(pts) >= 2 else None
    return (pts, dropped) if with_dropped else pts


@app.get("/api/links")
def links(lane: str = "main"):
    require_lane(lane)
    latest = view(lane, "traffic_latest")
    rows = q(f"""SELECT d.link_id, d.link_name, d.borough, d.free_flow_speed_mph, d.geometry_wkt,
                        l.speed_mph, l.traffic_state, l.valid_age_min, l.last_invalid_reason,
                        to_iso8601(l.last_valid_reading_ts_utc) AS last_valid_reading_utc
                 FROM lakehouse.serving.v_link_dim d LEFT JOIN {latest} l ON l.link_id = d.link_id""")
    feats, missing, repaired = [], [], []
    for r in map(dict, rows):          # bản sao: rows nằm trong cache, không được sửa tại chỗ
        coords, dropped = wkt_to_coords(r.pop("geometry_wkt"), with_dropped=True)
        if coords is None:
            missing.append(r["link_id"])
            continue
        r["geometry_points_dropped"] = dropped
        if dropped:
            repaired.append(r["link_id"])
        feats.append({"type": "Feature", "id": r["link_id"], "properties": r,
                      "geometry": {"type": "LineString", "coordinates": coords}})
    return {"type": "FeatureCollection", "features": feats, "links_without_geometry": missing,
            "links_geometry_repaired": repaired, "clock": clock(lane)}


@app.get("/api/geometry")
def geometry():
    """Chỉ hình học các đoạn đường (dùng cho hình nền). Không phụ thuộc làn, không đọc bảng Speed."""
    rows = q("SELECT link_id, borough, geometry_wkt FROM lakehouse.serving.v_link_dim")
    out = []
    for r in rows:
        coords = wkt_to_coords(r["geometry_wkt"])
        if coords:
            out.append({"id": r["link_id"], "borough": r["borough"], "c": [[round(x, 5), round(y, 5)] for x, y in coords]})
    return {"links": out, "n_links": len(rows)}


@app.get("/api/links/{link_id}/history")
def link_history(link_id: int, lane: str = "main", hours: int = Query(24, ge=1, le=168)):
    require_lane(lane)
    latest, enriched = view(lane, "traffic_latest"), view(lane, "traffic_5min_enriched")
    t = event_clock(lane)
    r = parallel(
        history=lambda: q(f"""SELECT to_iso8601(window_start_utc) AS t, avg_speed_mph, traffic_state AS state, n_records, n_valid,
                        layer, temperature_c, precipitation_mm
                 FROM {enriched}
                 WHERE link_id = {int(link_id)} AND window_start_utc >= {lit(t - dt.timedelta(hours=hours))}
                   AND window_start_utc <= {lit(t)}
                 ORDER BY window_start_utc LIMIT 3000""", ttl=180),
        info=lambda: q(f"""SELECT d.link_id, d.link_name, d.borough, d.free_flow_speed_mph, l.speed_mph, l.traffic_state,
                        l.valid_age_min, to_iso8601(l.last_valid_reading_ts_utc) AS last_valid_reading_utc
                 FROM lakehouse.serving.v_link_dim d LEFT JOIN {latest} l ON l.link_id = d.link_id
                 WHERE d.link_id = {int(link_id)}"""))
    if not r["info"]:
        raise HTTPException(404, f"Không có đoạn đường {link_id}")
    return {"link": r["info"][0], "history": r["history"], "hours": hours}


# ---------------------------------------------------------------- dự báo
# Không dùng view serving.v_*traffic_forecast_15m* ở đây: view JOIN cả view hợp nhất KHÔNG giới hạn thời gian (quét toàn
# bộ fact batch). Dashboard tự JOIN bảng dự báo với đúng khoảng cửa sổ cần (cùng định nghĩa actual/abs_error như view).
def forecast_table(lane: str) -> str:
    if lane == "main":
        return "lakehouse.ml.traffic_forecast_15m"
    lane_info = demo_lane()
    if not lane_info:
        raise HTTPException(404, "Chưa có phiên phát lại kiểm chứng")
    return lane_info["forecast_table"]


def forecast_cte(lane: str, since: dt.datetime, until: dt.datetime) -> str:
    unified = view(lane, "traffic_5min_unified")
    return f"""f AS (SELECT * FROM {forecast_table(lane)}
                    WHERE prediction_origin_utc >= {lit(since).replace(' UTC', '')}
                      AND prediction_origin_utc <= {lit(until).replace(' UTC', '')}),
       u AS (SELECT link_id, window_start_utc, avg_speed_mph, layer FROM {unified}
             WHERE window_start_utc >= {lit(since)} AND window_start_utc <= {lit(until + dt.timedelta(hours=1))} AND n_valid > 0),
       j AS (SELECT f.*, d.link_name, d.borough, u.avg_speed_mph AS actual_speed, u.layer AS actual_layer,
                    abs(u.avg_speed_mph - f.predicted_speed) AS abs_error
             FROM f LEFT JOIN lakehouse.serving.v_link_dim d ON d.link_id = f.link_id
             LEFT JOIN u ON u.link_id = f.link_id AND u.window_start_utc = CAST(f.target_window_start_utc AS timestamp(6) with time zone))"""


STOPPED_AFTER_MIN = int(os.getenv("DATA_STOPPED_AFTER_MINUTES", "30"))


def data_stopped(lane: str, c: dict) -> bool:
    """Nguồn dữ liệu của làn đã ngừng (phiên phát lại đã xong/dừng, hoặc không có bản ghi mới quá STOPPED_AFTER_MIN phút giờ máy).
    Khi đó cửa sổ đích nằm sau mốc số thực tế sẽ không bao giờ có số: không gọi là "chờ tới giờ đích"."""
    if lane == "demo":
        st, ln = latest_replay_status() or {}, demo_lane() or {}
        if st.get("run_id") and st.get("run_id") == ln.get("run_id") and st.get("state") in ("finished", "stopped", "failed"):
            return True
    p, n = parse_ts(c.get("processed_at_utc")), parse_ts(c.get("now_utc"))
    return bool(p and n and (n - p).total_seconds() > STOPPED_AFTER_MIN * 60)


def status_case(avail: str | None, alias: str = "f", ended: bool = False) -> str:
    wait = "BEYOND_DATA" if ended else "PENDING"   # đích sau mốc số thực tế: còn chờ được (đang phát) hay đã sau cuối dữ liệu
    pending = f"WHEN {alias}.target_window_end_utc > {lit(parse_ts(avail)).replace(' UTC', '')} THEN '{wait}'" if avail else f"WHEN true THEN '{wait}'"
    return f"""CASE WHEN {alias}.predicted_speed IS NULL THEN 'NO_FORECAST'
                    WHEN {alias}.actual_speed IS NOT NULL THEN 'EVALUATED'
                    {pending}
                    ELSE 'MISSING_ACTUAL' END"""


@app.get("/api/forecast")
def forecast(lane: str = "main", hours: int = Query(24, ge=1, le=168)):
    require_lane(lane)
    latest = view(lane, "traffic_latest")
    c = clock(lane)
    t = event_clock(lane)
    case = status_case(c.get("actual_available_utc"), "j", data_stopped(lane, c))
    cte = forecast_cte(lane, t - dt.timedelta(hours=hours), t + dt.timedelta(hours=1))
    r = parallel(
        rows=lambda: q(f"""WITH {cte}
                 SELECT j.link_id, j.link_name, j.borough, to_iso8601(j.prediction_origin_utc) AS origin_utc,
                        to_iso8601(j.target_window_start_utc) AS target_start_utc, to_iso8601(j.target_window_end_utc) AS target_end_utc,
                        j.predicted_speed, j.predicted_traffic_state, j.free_flow_speed, j.speed_last_known_60,
                        j.actual_speed, j.abs_error, j.model_version, j.feature_complete, j.weather_missing,
                        to_iso8601(j.created_at) AS created_at_utc, l.speed_mph AS current_speed, l.traffic_state AS current_state,
                        {case} AS status
                 FROM (SELECT j.*, row_number() OVER (PARTITION BY j.link_id ORDER BY j.prediction_origin_utc DESC, j.created_at DESC) AS rn
                       FROM j) j
                 LEFT JOIN {latest} l ON l.link_id = j.link_id
                 WHERE j.rn = 1 ORDER BY j.link_id"""),
        summary=lambda: q(f"""WITH {cte}
                 SELECT status, count(*) AS n, avg(abs_error) AS mae_model,
                        avg(abs(actual_speed - speed_last_known_60)) FILTER (WHERE status = 'EVALUATED') AS mae_persistence
                 FROM (SELECT j.*, {case} AS status FROM j) GROUP BY 1""", ttl=180),
        by_origin=lambda: q(f"""WITH {cte}
                 SELECT to_iso8601(prediction_origin_utc) AS origin_utc, count(*) AS n_forecast, count(actual_speed) AS n_evaluated,
                        avg(abs_error) AS mae_model, avg(abs(actual_speed - speed_last_known_60)) AS mae_persistence
                 FROM j WHERE predicted_speed IS NOT NULL GROUP BY 1 ORDER BY 1""", ttl=180),
        # Dự báo mới nhất luôn nhắm vào tương lai (chưa có số thực tế). Để bảng vẫn cho thấy độ đúng, lấy thêm cho mỗi đoạn
        # dự báo GẦN NHẤT ĐÃ ĐỐI CHIẾU (đã có cửa sổ đích thực tế), kèm sai số của mô hình và của persistence.
        last_eval=lambda: q(f"""WITH {cte}
                 SELECT link_id, to_iso8601(prediction_origin_utc) AS origin_utc, to_iso8601(target_window_start_utc) AS target_start_utc,
                        to_iso8601(target_window_end_utc) AS target_end_utc, predicted_speed, actual_speed, abs_error,
                        abs(actual_speed - speed_last_known_60) AS persistence_error
                 FROM (SELECT j.*, row_number() OVER (PARTITION BY link_id ORDER BY prediction_origin_utc DESC, created_at DESC) AS rn
                       FROM j WHERE predicted_speed IS NOT NULL AND actual_speed IS NOT NULL) e
                 WHERE rn = 1""", ttl=180))
    models = sorted({x["model_version"] for x in r["rows"] if x.get("model_version")})
    return {"lane": lane, "clock": c, "latest": r["rows"], "summary": r["summary"], "by_origin": r["by_origin"],
            "last_evaluated": r["last_eval"], "data_stopped": data_stopped(lane, c), "models": models, "hours": hours, "forecast_table": forecast_table(lane)}


@app.get("/api/forecast/{link_id}/history")
def forecast_history(link_id: int, lane: str = "main", hours: int = Query(24, ge=1, le=168)):
    require_lane(lane)
    enriched = view(lane, "traffic_5min_enriched")
    c = clock(lane)
    t = event_clock(lane)
    case = status_case(c.get("actual_available_utc"), "j", data_stopped(lane, c))
    cte = forecast_cte(lane, t - dt.timedelta(hours=hours), t + dt.timedelta(hours=1))
    r = parallel(
        predictions=lambda: q(f"""WITH {cte}
                  SELECT to_iso8601(j.prediction_origin_utc) AS origin_utc, to_iso8601(j.target_window_start_utc) AS target_start_utc,
                         j.predicted_speed, j.actual_speed, j.abs_error, j.speed_last_known_60, j.model_version, {case} AS status
                  FROM j WHERE j.link_id = {int(link_id)} ORDER BY j.prediction_origin_utc LIMIT 3000"""),
        actual=lambda: q(f"""SELECT to_iso8601(window_start_utc) AS t, avg_speed_mph, n_valid, layer FROM {enriched}
                   WHERE link_id = {int(link_id)} AND window_start_utc >= {lit(t - dt.timedelta(hours=hours))}
                     AND window_start_utc <= {lit(t)} ORDER BY 1 LIMIT 3000""", ttl=180))
    return {"link_id": link_id, **r, "hours": hours}


# ---------------------------------------------------------------- đánh giá mô hình (artifact thật)
def latest_ml_version() -> str | None:
    """Phiên bản đánh giá của mô hình đang phục vụ (models/ml/current.json, vd lgbm-v2-... -> v2);
    không xác định được thì lấy thư mục reports/ml/vN có N lớn nhất. Các phiên bản cũ vẫn nằm trên đĩa, dashboard không hiện."""
    have = {p.name for p in REPORTS_DIR.iterdir() if (p / "metrics.json").exists()} if REPORTS_DIR.exists() else set()
    current = MODELS_DIR / "current.json"
    if current.exists():
        mv = re.search(r"-(v\d+)-", str(read_json(current).get("model_version", "")))
        if mv and mv.group(1) in have:
            return mv.group(1)
    nums = sorted((int(v[1:]), v) for v in have if re.fullmatch(r"v\d+", v))
    return nums[-1][1] if nums else None


@app.get("/api/ml")
def ml():
    def run():
        version = latest_ml_version()
        if version is None:
            raise HTTPException(404, f"Không có artifact đánh giá trong {REPORTS_DIR}")
        m = read_json(REPORTS_DIR / version / "metrics.json")
        final = m["folds"]["final_test"]
        exp = {}
        for r in m.get("lgbm_results", []):
            if r.get("experiment") in ("ablation", "spatial", "weather_lag", "avail_lag", "weighting", "sliding", "walk_forward"):
                exp.setdefault(r["experiment"], []).append(
                    {k: r.get(k) for k in ("fold", "model", "n", "mae", "rmse", "holdout_links")})
        current = MODELS_DIR / "current.json"
        model_meta = None
        if current.exists():
            md_path = MODELS_DIR / read_json(current)["model_version"] / "metadata.json"
            if md_path.exists():
                md = read_json(md_path)
                model_meta = {k: md.get(k) for k in ("model_version", "feature_version", "train_start", "train_boundary",
                                                     "baseline_as_of_utc", "horizon_minutes", "availability_lag_minutes",
                                                     "weather_lag_minutes", "dataset_rows", "lightgbm_version", "created_at")}
                model_meta["n_features"] = len(md.get("features", []))
        return {"version": version, "model": model_meta,
                "final_test": {name: {k: v for k, v in d.items() if k != "state_classification"} |
                               {"macro_f1": d.get("state_classification", {}).get("macro_f1")}
                               for name, d in final.items()},
                "classification": final["lightgbm"].get("state_classification"),
                "folds": {f: {name: {k: d.get(k) for k in ("n", "mae", "rmse")} |
                              {"macro_f1": d.get("state_classification", {}).get("macro_f1")}
                              for name, d in fv.items()} for f, fv in m["folds"].items() if f != "final_test"},
                "walk_forward": m.get("walk_forward_summary"), "experiments": exp,
                "link_mae_distribution": m.get("focus_extra", {}).get("link_mae_distribution")}
    return cached(("ml",), 300, run)


# ---------------------------------------------------------------- vận hành
def airflow_token():
    def run():
        body = json.dumps({"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}).encode()
        return http_json(f"{AIRFLOW_URL}/auth/token", body, {"Content-Type": "application/json"})["access_token"]
    return cached(("af_token",), 600, run)


def airflow_get(path: str):
    return http_json(f"{AIRFLOW_URL}{path}", headers={"Authorization": f"Bearer {airflow_token()}"})


def airflow_status():
    out = []
    for dag in DAGS:
        try:
            d = airflow_get(f"/api/v2/dags/{dag}")
        except urllib.error.HTTPError as e:
            out.append({"dag_id": dag, "error": f"HTTP {e.code}"})
            continue
        runs = airflow_get(f"/api/v2/dags/{dag}/dagRuns?order_by=-run_after&limit=8")["dag_runs"]
        tasks = []
        if runs:
            rid = urllib.parse.quote(runs[0]["dag_run_id"], safe="")
            tasks = [{"task_id": t["task_id"], "state": t["state"], "start": t.get("start_date"), "end": t.get("end_date"),
                      "try_number": t.get("try_number")}
                     for t in airflow_get(f"/api/v2/dags/{dag}/dagRuns/{rid}/taskInstances?limit=50")["task_instances"]]
        out.append({"dag_id": dag, "is_paused": d.get("is_paused"), "schedule": d.get("timetable_summary"),
                    "runs": [{"run_id": r["dag_run_id"], "state": r["state"], "start": r.get("start_date"),
                              "end": r.get("end_date")} for r in runs], "latest_tasks": tasks})
    return out


def prom(expr: str):
    url = f"{PROMETHEUS_URL}/api/v1/query?query={urllib.parse.quote(expr)}"
    return http_json(url)["data"]["result"]


def kafka_status():
    offsets = prom("sum by (topic) (kafka_topic_partition_current_offset)")
    brokers = prom("kafka_brokers")
    targets = http_json(f"{PROMETHEUS_URL}/api/v1/targets")["data"]["activeTargets"]
    return {"topics": {r["metric"].get("topic"): float(r["value"][1]) for r in offsets
                       if not r["metric"].get("topic", "").startswith("__")},
            "brokers": float(brokers[0]["value"][1]) if brokers else None,
            "targets": [{"job": t["labels"].get("job"), "instance": t["labels"].get("instance"), "health": t["health"],
                         "last_error": t.get("lastError") or None} for t in targets]}


def spark_status():
    j = http_json(f"{SPARK_MASTER_URL}/json/")
    return {"status": j.get("status"), "cores": j.get("cores"), "cores_used": j.get("coresused"),
            "memory_mb": j.get("memory"), "memory_used_mb": j.get("memoryused"),
            "workers": [{"id": w["id"], "state": w["state"], "cores": w["cores"], "cores_used": w["coresused"],
                         "memory_mb": w["memory"], "memory_used_mb": w["memoryused"],
                         "last_heartbeat_ms": w.get("lastheartbeat")} for w in j.get("workers", [])],
            "apps": [{"name": a["name"], "state": a["state"], "cores": a["cores"], "started_ms": a.get("starttime"),
                      "duration_ms": a.get("duration")} for a in j.get("activeapps", [])]}


def demo_lane() -> dict | None:
    f = OPS_LOG_DIR / "demo_lane.json"
    try:
        return read_json(f) if f.exists() else None
    except (OSError, json.JSONDecodeError):
        return None


def progress(log_dir: Path) -> dict:
    last = {}
    for line in tail_lines(log_dir / "speed_progress.jsonl", 300):
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        last[r.get("query")] = {k: r.get(k) for k in ("ts", "batch_id", "num_input_rows", "input_rows_per_sec",
                                                         "processed_rows_per_sec", "trigger_execution_ms", "watermark",
                                                         "max_offsets_behind", "state_rows", "rows_dropped_by_watermark")}
    return last


def streaming_status():
    out = {"queries": progress(OPS_LOG_DIR), "supervisor": [l.strip() for l in tail_lines(OPS_LOG_DIR / "speed_supervisor.log", 3)]}
    lane = demo_lane()
    if lane:
        d = OPS_LOG_DIR / "demo" / lane["run_id"]
        out["demo"] = {"run_id": lane["run_id"], "queries": progress(d),
                       "supervisor": [l.strip() for l in tail_lines(d / "speed_supervisor.log", 3)]}
    return out


def lambda_status():
    st = q("""SELECT to_iso8601(batch_state) AS batch_state, published_handoff_id, to_iso8601(published_at) AS published_at,
                     last_handoff_id, last_status, to_iso8601(last_batch_state) AS last_batch_state, last_message
              FROM lakehouse.serving.v_lambda_status""", ttl=150)[0]
    hist = [dict(h) for h in q("""SELECT handoff_id, status, to_iso8601(batch_state) AS batch_state,
                                         to_iso8601(created_at) AS created_at, checks
                                  FROM lakehouse.gold.lambda_control ORDER BY created_at DESC LIMIT 12""", ttl=150)]
    for h in hist:
        try:
            c = json.loads(h.pop("checks") or "{}")
        except json.JSONDecodeError:
            c = {}
        h["checks"] = {k: c.get(k) for k in ("complete", "unique_grain", "hourly_consistent", "covers_realtime", "ok",
                                              "silver_rows", "batch_sum_n_records", "rt_windows_without_batch")}
    return {"current": st, "history": hist}


def iceberg_status():
    tables = ["gold.fact_traffic_5min", "gold.fact_traffic_hourly", "speed.traffic_5min_rt", "speed.traffic_latest",
              "ml.traffic_forecast_15m"]
    lane = demo_lane()
    if lane:
        tables += [f"{lane['speed_namespace'].split('.', 1)[1]}.traffic_5min_rt", lane["forecast_table"].split(".", 1)[1]]
    out = {}
    for t in tables:
        schema, name = t.split(".")
        r = safe(lambda s=schema, n=name: q(f"""SELECT to_iso8601(committed_at) AS committed_at, operation,
                         element_at(summary, 'total-records') AS total_records
                  FROM lakehouse.{s}."{n}$snapshots" ORDER BY committed_at DESC LIMIT 1""", ttl=180))
        out[t] = (r[0] if r else None) if isinstance(r, list) else None
    return out


def inference_status():
    runs = []
    for line in reversed(tail_lines(OPS_LOG_DIR / "inference_runs.jsonl", 40)):
        try:
            runs.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return {"runs": runs[:15], "note": None if runs else "Chưa có lượt suy luận nào (log inference_runs.jsonl trống)"}


def latest_replay_status():
    files = sorted(OPS_LOG_DIR.glob("replay_status_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    try:
        return read_json(files[0])
    except (OSError, json.JSONDecodeError) as e:
        return {"error": str(e)}


@app.get("/api/ops")
def ops():
    def run():
        t0 = time.time()
        ping = safe(lambda: (query("SELECT 1"), round((time.time() - t0) * 1000))[1])
        r = parallel(kafka=lambda: safe(kafka_status), spark=lambda: safe(spark_status), airflow=lambda: safe(airflow_status),
                     lam=lambda: safe(lambda_status))
        return {"trino_ping_ms": ping, "kafka": r["kafka"], "spark": r["spark"], "streaming": safe(streaming_status),
                "airflow": r["airflow"], "lambda": r["lam"], "iceberg": safe(iceberg_status),
                "inference": safe(inference_status), "replay": latest_replay_status(),
                "generated_in_ms": round((time.time() - t0) * 1000)}
    return cached(("ops",), 120, run)


@app.get("/api/health")
def health():
    return {"ok": True, "warmed_at": _warm.get("at"), "warm_seconds": _warm.get("seconds"), "warm_error": _warm.get("error")}


# ---------------------------------------------------------------- làm ấm cache nền
# Giao diện tự làm mới mỗi 60 s; nếu mỗi lần đều gặp cache hết hạn thì người xem phải chờ 10-30 s.
# Luồng nền tính sẵn các endpoint chính mỗi WARM_INTERVAL giây (một bộ truy vấn, tuần tự) -> giao diện luôn đọc cache.
WARM_INTERVAL = int(os.getenv("WARM_INTERVAL", "30"))
_warm: dict = {}


def _warm_loop():
    time.sleep(5)
    while True:
        t0 = time.time()
        _force.on = True
        try:
            for ln in [k for k in LANES if safe(lambda k=k: lane_exists(k)) is True]:
                for fn in (lambda: clock(ln), lambda: overview(ln, 6), lambda: links(ln), lambda: forecast(ln, 24),
                           lambda: ops_center.timeline(ln, 2), lambda: ops_center.handoffs(ln),
                           lambda: ops_center.flow(ln)):
                    safe(fn)
            safe(ops)
            _warm.update(at=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                         seconds=round(time.time() - t0, 1), error=None)
        except Exception as e:  # noqa: BLE001
            _warm.update(error=f"{type(e).__name__}: {e}")
        finally:
            _force.on = False
        time.sleep(max(5.0, WARM_INTERVAL - (time.time() - t0)))


if os.getenv("CACHE_WARMER", "1") == "1":
    threading.Thread(target=_warm_loop, name="cache-warmer", daemon=True).start()


@app.exception_handler(TrinoError)
def trino_error(_, exc: TrinoError):
    return JSONResponse(status_code=502, content={"detail": f"Trino: {exc}"})


# ---------------------------------------------------------------- trung tâm vận hành (/api/ops/*)
import ops_center  # noqa: E402  (dùng các hàm phía trên)

app.include_router(ops_center.router)


# ---------------------------------------------------------------- frontend tĩnh (SPA)
if (WEB_DIR / "assets").is_dir():
    app.mount("/assets", StaticFiles(directory=WEB_DIR / "assets"), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str):
        f = WEB_DIR / path
        if path and f.is_file():
            return FileResponse(f)
        return FileResponse(WEB_DIR / "index.html")
