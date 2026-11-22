"""Quy tắc dùng chung cho Serving API. Phải khớp với serving.v_traffic_latest (jobs/serving/trino_views.sql)
và common/quality_rules.py của speed layer: CONGESTED < 0,5 <= SLOW < 0,8 <= FREE_FLOW (speed / free_flow_speed).
"""
import json
import math
import os

CONGESTED_RATIO = 0.5
SLOW_RATIO = 0.8
STALE_AFTER_MINUTES = int(os.getenv("STALE_AFTER_MINUTES", "15"))
NY_TZ = "America/New_York"

# Trạng thái có số đo hợp lệ (dùng để tính tốc độ trung bình, top tắc...)
MEASURED_STATES = ("FREE_FLOW", "SLOW", "CONGESTED")
ALL_STATES = ("FREE_FLOW", "SLOW", "CONGESTED", "STALE", "NO_VALID_DATA", "NO_BASELINE", "NO_DATA")


def classify(speed, free_flow, valid_age_min=None) -> str:
    if speed is None:
        return "NO_VALID_DATA"
    if valid_age_min is not None and valid_age_min > STALE_AFTER_MINUTES:
        return "STALE"
    if not free_flow or free_flow <= 0:
        return "NO_BASELINE"
    r = speed / free_flow
    if r < CONGESTED_RATIO:
        return "CONGESTED"
    if r < SLOW_RATIO:
        return "SLOW"
    return "FREE_FLOW"


def parse_link_points(text: str | None) -> list[list[float]]:
    """'lat,lon lat,lon ...' -> [[lon, lat], ...] (thứ tự GeoJSON).

    Nguồn NYC DOT cắt chuỗi ở 255 ký tự nên cặp cuối có thể cụt -> bỏ cặp không hợp lệ.
    """
    pts = []
    for tok in (text or "").split():
        try:
            a, b = tok.split(",")
            lat, lon = float(a), float(b)
        except ValueError:
            continue
        if 40.3 < lat < 41.2 and -74.5 < lon < -73.5:
            pts.append([lon, lat])
    return pts


def length_km(coords: list[list[float]]) -> float:
    total = 0.0
    for (lon1, lat1), (lon2, lat2) in zip(coords, coords[1:]):
        p1, p2 = math.radians(lat1), math.radians(lat2)
        h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
        total += 2 * 6371 * math.asin(math.sqrt(h))
    return total


def cell_lat_lon(cell_id: str) -> tuple[float | None, float | None]:
    """cell_id dạng '40.5272_-74.2373' (tên file ifs_<lat>_<lon>.json)."""
    try:
        a, b = cell_id.split("_")
        return float(a), float(b)
    except (ValueError, AttributeError):
        return None, None


def geometry(coords: list[list[float]], lat=None, lon=None) -> dict:
    if len(coords) >= 2:
        return {"type": "LineString", "coordinates": coords}
    if lat is not None and lon is not None:
        return {"type": "Point", "coordinates": [lon, lat]}
    return {"type": "LineString", "coordinates": []}


def dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))
