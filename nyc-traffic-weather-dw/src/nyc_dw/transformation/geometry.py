"""Lam sach link_points, tinh diem dai dien va WKT; haversine."""
import math, re, statistics as st

TOKEN_RE = re.compile(r"^-?\d{1,3}\.\d+,-?\d{1,3}\.\d+$")


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    p = math.pi / 180
    a = math.sin((lat2 - lat1) * p / 2) ** 2 + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2
    return 2 * 6371.0088 * math.asin(math.sqrt(a))


def clean_points(link_points: str, bbox: dict, max_dev_km: float) -> dict:
    """Tra ve diem hop le (giu thu tu), so token loi, so diem bi loai vi lech xa, diem dai dien (trung vi)."""
    tokens = link_points.split()
    pts, bad = [], []
    for t in tokens:
        if not TOKEN_RE.match(t):
            bad.append(t); continue
        la, lo = map(float, t.split(","))
        if bbox["lat_min"] < la < bbox["lat_max"] and bbox["lon_min"] < lo < bbox["lon_max"]:
            pts.append((la, lo))
        else:
            bad.append(t)
    far = []
    if pts:
        mla, mlo = st.median(p[0] for p in pts), st.median(p[1] for p in pts)
        keep = [p for p in pts if haversine_km(mla, mlo, *p) <= max_dev_km]
        far = [p for p in pts if p not in keep]
        pts = keep
    rep = (st.median(p[0] for p in pts), st.median(p[1] for p in pts)) if pts else (None, None)
    length = sum(haversine_km(*pts[i], *pts[i + 1]) for i in range(len(pts) - 1)) if len(pts) > 1 else 0.0
    if len(pts) >= 2:
        wkt = "LINESTRING (" + ", ".join(f"{lo} {la}" for la, lo in pts) + ")"
    elif len(pts) == 1:
        wkt = f"POINT ({pts[0][1]} {pts[0][0]})"
    else:
        wkt = None
    return {"n_points_raw": len(tokens), "n_points_valid": len(pts), "n_tokens_bad": len(bad), "n_points_far_dropped": len(far),
            "bad_tokens": bad, "rep_lat": rep[0], "rep_lon": rep[1], "length_km_valid": round(length, 3), "geometry_wkt": wkt,
            "start": pts[0] if pts else (None, None), "end": pts[-1] if pts else (None, None)}
