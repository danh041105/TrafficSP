"""Hàm dùng chung cho dữ liệu demo. Đọc đúng dữ liệu thô của nhóm trong data/raw (giống producer)."""
import json
import os
from pathlib import Path

import clickhouse_connect
import numpy as np
import pandas as pd

ROOT = Path(os.getenv("TRAFFICSP_ROOT", Path(__file__).resolve().parents[2]))
RAW_TRAFFIC_DIR = ROOT / "data/raw/nyc_dot"
RAW_WEATHER_DIR = ROOT / "data/raw/open_meteo/ecmwf_ifs"
NY = "America/New_York"

# Khớp common/config.py (FREE_FLOW_*) bên speed layer
FREE_FLOW_PERCENTILE = 0.85
FREE_FLOW_LOCAL_HOURS = (22, 23, 0, 1, 2, 3, 4, 5)
FREE_FLOW_MIN_SAMPLES = 100


def ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("DEMO_CH_HOST", "localhost"),
        port=int(os.getenv("DEMO_CH_PORT", "8123")),
        username=os.getenv("DEMO_CH_USER", "demo"),
        password=os.getenv("DEMO_CH_PASSWORD", "demo"),
    )


def apply_schema(client):
    sql = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")
    for stmt in sql.split(";"):
        body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip()
        if body:
            client.command(body)


def read_traffic_month(month: int, columns=("link_id", "speed", "travel_time", "status", "data_as_of")):
    """Đọc 1 tháng, đổi data_as_of (giờ New York) sang UTC.
    Giống Silver: bỏ status=-101, bỏ giờ DST lặp/không tồn tại; speed<=0 là không hợp lệ."""
    df = pd.read_parquet(RAW_TRAFFIC_DIR / f"2025-{month:02d}.parquet", columns=list(columns))
    df = df[df["status"].astype(int) != -101].copy()
    df["link_id"] = df["link_id"].astype("int64")
    local = pd.to_datetime(df["data_as_of"])
    df["local_ts"] = local
    df["event_time"] = local.dt.tz_localize(NY, ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
    df = df.dropna(subset=["event_time"])
    df["is_valid"] = (df["speed"] > 0).astype("uint8")
    return df


def read_weather() -> pd.DataFrame:
    frames = []
    for path in sorted(RAW_WEATHER_DIR.glob("ifs_*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        h = data["hourly"]
        frames.append(pd.DataFrame({
            "cell_id": path.stem.removeprefix("ifs_"),
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "time_utc": pd.to_datetime(h["time"]).tz_localize("UTC"),
            "temperature_c": h["temperature_2m"],
            "precipitation_mm": h["precipitation"],
            "wind_speed_kmh": h["wind_speed_10m"],
            "weather_code": h["weather_code"],
        }))
    return pd.concat(frames, ignore_index=True)


def median_point(link_points: str):
    pts = []
    for tok in str(link_points).split():
        try:
            a, b = tok.split(",")
            pts.append((float(a), float(b)))
        except ValueError:
            continue
    if not pts:
        return np.nan, np.nan
    arr = np.array(pts)
    return float(np.median(arr[:, 0])), float(np.median(arr[:, 1]))
