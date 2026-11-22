"""Nạp dữ liệu nền cho CHẾ ĐỘ DEMO từ data/raw (giai đoạn 1: 01-09/2025):
  dim_link (vị trí, ô thời tiết, free-flow P85), hourly (lịch sử theo giờ), weather (16 ô, cả năm).
Chạy:  docker compose --profile demo run --rm serving-demo-load
       (hoặc trên máy: python -m serving.demo.load_demo)
"""
import numpy as np
import pandas as pd

from serving.demo.common import (FREE_FLOW_LOCAL_HOURS, FREE_FLOW_MIN_SAMPLES, FREE_FLOW_PERCENTILE,
                                 RAW_TRAFFIC_DIR, apply_schema, ch_client, median_point,
                                 read_traffic_month, read_weather)

HISTORY_MONTHS = range(1, 10)


def build_links(weather: pd.DataFrame) -> pd.DataFrame:
    # Gom từng tháng xuống mức link trước khi gộp, tránh giữ hàng triệu chuỗi link_points trong RAM
    parts = []
    for m in HISTORY_MONTHS:
        df = pd.read_parquet(RAW_TRAFFIC_DIR / f"2025-{m:02d}.parquet",
                             columns=["link_id", "link_name", "borough", "link_points", "data_as_of"])
        df = df.drop_duplicates(["link_id", "link_name", "borough", "link_points"], keep="last")
        parts.append(df)
    meta = pd.concat(parts, ignore_index=True)
    meta["link_id"] = meta["link_id"].astype("int64")
    meta["pts_len"] = meta["link_points"].fillna("").str.len()
    # tên/quận lấy bản mới nhất; link_points lấy chuỗi dài nhất (đỡ bị cắt nhất)
    latest = meta.sort_values("data_as_of").groupby("link_id").tail(1).set_index("link_id")
    longest = meta.sort_values("pts_len").groupby("link_id").tail(1).set_index("link_id")["link_points"]
    del meta, parts
    links = latest[["link_name", "borough"]].join(longest).reset_index()
    links[["latitude", "longitude"]] = links["link_points"].apply(lambda s: pd.Series(median_point(s)))
    cells = weather.groupby("cell_id")[["latitude", "longitude"]].first()
    def nearest(row):
        d = (cells.latitude - row.latitude) ** 2 + ((cells.longitude - row.longitude) * np.cos(np.radians(40.7))) ** 2
        return d.idxmin()
    links["cell_id"] = links.apply(nearest, axis=1)
    return links


def main():
    client = ch_client()
    apply_schema(client)
    weather = read_weather()
    links = build_links(weather)

    hourly_parts, ff_parts = [], []
    for m in HISTORY_MONTHS:
        df = read_traffic_month(m)
        v = df[df.is_valid == 1]
        h = v.assign(hour_utc=v.event_time.dt.floor("h")).groupby(["link_id", "hour_utc"]) \
             .agg(sum_speed=("speed", "sum"), n_valid=("speed", "size")).reset_index()
        hourly_parts.append(h)
        ff_parts.append(v[v.local_ts.dt.hour.isin(FREE_FLOW_LOCAL_HOURS)][["link_id", "speed"]])
        print(f"2025-{m:02d}: {len(df):,} bản ghi, {len(h):,} dòng giờ")

    ff = pd.concat(ff_parts).groupby("link_id")["speed"].agg(["count", lambda s: s.quantile(FREE_FLOW_PERCENTILE)])
    ff.columns = ["n", "p85"]
    links["free_flow_speed"] = links.link_id.map(ff.p85.where(ff.n >= FREE_FLOW_MIN_SAMPLES))

    for t in ("dim_link", "hourly", "weather", "reading", "forecast"):
        client.command(f"TRUNCATE TABLE traffic.{t}")
    client.insert_df("traffic.dim_link", links[["link_id", "link_name", "borough", "free_flow_speed", "cell_id",
                                                "latitude", "longitude", "link_points"]].fillna({"link_points": ""}))
    hourly = pd.concat(hourly_parts, ignore_index=True)
    client.insert_df("traffic.hourly", hourly)
    w = weather.drop(columns=["latitude", "longitude"])
    w["weather_code"] = w.weather_code.astype("float").astype("UInt16")
    client.insert_df("traffic.weather", w)
    print(f"dim_link {len(links)} (có free-flow: {links.free_flow_speed.notna().sum()}), "
          f"hourly {len(hourly):,}, weather {len(w):,}")


if __name__ == "__main__":
    main()
