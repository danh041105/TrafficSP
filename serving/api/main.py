"""Serving API cho dashboard giao thông + thời tiết NYC.

Nguồn dữ liệu chọn bằng biến môi trường SERVING_SOURCE:
  trino (mặc định) - đọc view lakehouse.serving.* qua Trino (kiến trúc thật)
  demo             - ClickHouse cục bộ + replay parquet (serving/demo), cho máy không chạy nổi cả stack

Chạy:  uvicorn serving.api.main:app --port 8000      (từ thư mục gốc repo)
Docs:  http://localhost:8000/docs
"""
import os
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from .rules import (MEASURED_STATES, cell_lat_lon, geometry, length_km, parse_link_points)
from .metrics import install as install_metrics
from .sources.base import Source


def make_source() -> Source:
    kind = os.getenv("SERVING_SOURCE", "trino").lower()
    if kind == "demo":
        from .sources.demo_source import DemoSource
        return DemoSource()
    from .sources.trino_source import TrinoSource
    return TrinoSource()


NY_ZONE = ZoneInfo("America/New_York")
src = make_source()
app = FastAPI(title="TrafficSP Serving API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])


# ---------------------------------------------------------------- cache đơn giản theo TTL
def ttl_cache(seconds: float):
    def deco(fn):
        store: dict = {}

        @wraps(fn)
        def wrapper(*args):
            now = time.monotonic()
            hit = store.get(args)
            if hit and now - hit[0] < seconds:
                return hit[1]
            val = fn(*args)
            store[args] = (now, val)
            return val
        return wrapper
    return deco


def iso(v):
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).isoformat()
    return v


def clean(rows):
    return [{k: iso(v) for k, v in r.items()} for r in rows]


def num(v):
    return None if v is None else float(v)


@ttl_cache(300)
def links_by_id() -> dict:
    out = {}
    for r in src.links():
        coords = parse_link_points(r.get("link_points"))
        out[int(r["link_id"])] = {
            "link_id": str(r["link_id"]),
            "link_name": r.get("link_name") or "",
            "borough": r.get("borough") or "",
            "free_flow_speed_mph": num(r.get("free_flow_speed")),
            "cell_id": r.get("cell_id"),
            "length_km": round(length_km(coords), 3) if len(coords) > 1 else None,
            "geometry": geometry(coords, r.get("latitude"), r.get("longitude")),
        }
    return out


@ttl_cache(5)
def clock():
    return src.clock()


@ttl_cache(5)
def latest_joined() -> list[dict]:
    dims = links_by_id()
    seen, out = set(), []
    for r in src.latest():
        lid = int(r["link_id"])
        d = dims.get(lid)
        if d is None:
            continue
        seen.add(lid)
        speed = num(r.get("speed"))
        ff = d["free_flow_speed_mph"]
        out.append({**d, "speed_mph": speed, "travel_time_s": r.get("travel_time"),
                    "speed_ratio": speed / ff if speed is not None and ff else None,
                    "traffic_state": r.get("traffic_state") or "NO_DATA",
                    "event_time": iso(r.get("event_time"))})
    for lid, d in dims.items():           # link có trong dimension nhưng chưa nhận dữ liệu
        if lid not in seen:
            out.append({**d, "speed_mph": None, "travel_time_s": None, "speed_ratio": None,
                        "traffic_state": "NO_DATA", "event_time": None})
    return out


@ttl_cache(600)
def profile(link_id, borough):
    return [{"dow": int(r["dow"]), "hour": int(r["hour"]), "speed": round(float(r["speed"]), 1)}
            for r in src.dow_hour_profile(link_id, borough) if r["speed"] is not None]


def require_clock():
    c = clock()
    if c is None:
        raise HTTPException(503, "Chưa có dữ liệu realtime (speed layer / replay chưa chạy)")
    return c if c.tzinfo else c.replace(tzinfo=timezone.utc)


# số đo Prometheus: /metrics (xem serving/api/metrics.py)
install_metrics(app, src.name, latest_joined, clock)

# ---------------------------------------------------------------- endpoints
@app.get("/api/health")
def health():
    try:
        c = clock()
        return {"ok": True, "source": src.name, "clock": iso(c)}
    except Exception as e:  # noqa: BLE001 - health phải luôn trả lời
        return {"ok": False, "source": src.name, "error": str(e)[:300]}


@app.get("/api/meta")
def meta():
    return {"source": src.name, "clock": iso(clock()), "links": len(links_by_id())}


@app.get("/api/map/current")
def map_current(borough: str | None = None):
    feats = []
    for r in latest_joined():
        if borough and r["borough"] != borough:
            continue
        props = {k: v for k, v in r.items() if k != "geometry"}
        feats.append({"type": "Feature", "geometry": r["geometry"], "properties": props})
    return {"type": "FeatureCollection", "features": feats}


@app.get("/api/kpi/now")
def kpi_now(borough: str | None = None):
    rows = [r for r in latest_joined() if not borough or r["borough"] == borough]
    measured = [r for r in rows if r["traffic_state"] in MEASURED_STATES and r["speed_mph"] is not None]
    return {
        "links_total": len(rows),
        "links_reporting": len(measured),
        "city_avg_speed": round(sum(r["speed_mph"] for r in measured) / len(measured), 1) if measured else None,
        "congested_links": sum(r["traffic_state"] == "CONGESTED" for r in rows),
        "slow_links": sum(r["traffic_state"] == "SLOW" for r in rows),
        "no_data_links": len(rows) - len(measured),
        "clock": iso(clock()),
        "source": src.name,
    }


@app.get("/api/kpi/states")
def kpi_states(borough: str | None = None):
    counts: dict[str, int] = {}
    for r in latest_joined():
        if not borough or r["borough"] == borough:
            counts[r["traffic_state"]] = counts.get(r["traffic_state"], 0) + 1
    return [{"traffic_state": k, "links": v} for k, v in sorted(counts.items(), key=lambda kv: -kv[1])]


@app.get("/api/kpi/boroughs")
def kpi_boroughs():
    groups: dict[str, list] = {}
    for r in latest_joined():
        groups.setdefault(r["borough"] or "?", []).append(r)
    out = []
    for b, rows in sorted(groups.items()):
        m = [r for r in rows if r["traffic_state"] in MEASURED_STATES and r["speed_mph"] is not None]
        out.append({"borough": b, "links": len(rows), "reporting": len(m),
                    "congested": sum(r["traffic_state"] == "CONGESTED" for r in rows),
                    "avg_speed": round(sum(r["speed_mph"] for r in m) / len(m), 1) if m else None})
    return out


@app.get("/api/links")
def link_list(borough: str | None = None):
    return sorted(({"link_id": d["link_id"], "link_name": d["link_name"], "borough": d["borough"]}
                   for d in links_by_id().values() if not borough or d["borough"] == borough),
                  key=lambda d: d["link_name"])


@app.get("/api/links/top-congested")
def top_congested(limit: int = Query(10, ge=1, le=50), borough: str | None = None):
    rows = [r for r in latest_joined()
            if r["speed_ratio"] is not None and r["traffic_state"] in MEASURED_STATES
            and (not borough or r["borough"] == borough)]
    rows.sort(key=lambda r: r["speed_ratio"])
    return [{k: v for k, v in r.items() if k != "geometry"} for r in rows[:limit]]


@app.get("/api/links/{link_id}")
def link_detail(link_id: int):
    r = next((x for x in latest_joined() if x["link_id"] == str(link_id)), None)
    if r is None:
        raise HTTPException(404, "Không có link này")
    out = {k: v for k, v in r.items() if k != "geometry"}
    c = clock()
    w = next((x for x in src.weather_at(c) if x["cell_id"] == r["cell_id"]), None) if c and r["cell_id"] else None
    out["weather"] = clean([w])[0] if w else None
    return out


@app.get("/api/links/{link_id}/history")
def link_history(link_id: int, hours: int = Query(24, ge=1, le=168)):
    c = require_clock()
    return clean(src.history_15min(link_id, c - timedelta(hours=hours), c + timedelta(minutes=1)))


@app.get("/api/links/{link_id}/forecast")
def link_forecast(link_id: int):
    return clean(src.forecast(link_id))


@app.get("/api/links/{link_id}/heatmap")
def link_heatmap(link_id: int):
    return profile(link_id, None)


@app.get("/api/city/heatmap")
def city_heatmap(borough: str | None = None):
    return profile(None, borough)


@app.get("/api/city/today")
def city_today(borough: str | None = None, hours: int = Query(24, ge=1, le=168)):
    """Tốc độ toàn mạng 15 phút trong `hours` giờ trước đồng hồ dữ liệu, kèm trung bình lịch sử cùng thứ/giờ."""
    c = require_clock()
    hist = {(p["dow"], p["hour"]): p["speed"] for p in profile(None, borough)}
    out = []
    for r in src.history_15min(None, c - timedelta(hours=hours), c + timedelta(minutes=1), borough):
        t = r["t"] if r["t"].tzinfo else r["t"].replace(tzinfo=timezone.utc)
        local = t.astimezone(NY_ZONE)
        out.append({"t": iso(t), "speed": num(r["speed"]), "hist": hist.get((local.isoweekday(), local.hour))})
    return out


@app.get("/api/weather/current")
def weather_current():
    c = require_clock()
    out = []
    for w in src.weather_at(c):
        lat, lon = cell_lat_lon(w["cell_id"])
        out.append({**{k: iso(v) for k, v in w.items()}, "latitude": lat, "longitude": lon})
    return out

