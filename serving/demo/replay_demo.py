"""Giả lập speed layer cho CHẾ ĐỘ DEMO: phát lại dữ liệu giai đoạn 2 (10-12/2025) vào ClickHouse
theo đồng hồ sự kiện, GIỮ NGUYÊN timestamp 2025 (giống producer gd2 + speed layer thật).

Kèm dự báo baseline (trộn tốc độ hiện tại với trung bình lịch sử cùng thứ/giờ) để giao diện có đường forecast
cho tới khi phần MLOps có view serving.v_traffic_forecast. Tắt bằng --no-forecast.

Chạy:  docker compose --profile demo up -d serving-demo-replay
       python -m serving.demo.replay_demo --start "2025-10-07 05:00" --speed 20
"""
import argparse
import time
from datetime import timedelta

import numpy as np
import pandas as pd

from serving.demo.common import NY, apply_schema, ch_client, read_traffic_month

HORIZONS = {15: 0.7, 30: 0.5, 60: 0.3}   # phút -> trọng số tốc độ hiện tại


def load_months(start_utc: pd.Timestamp) -> pd.DataFrame:
    # Chỉ đọc tháng chứa mốc bắt đầu (+ tháng của 24h nạp trước nếu khác tháng) để khởi động nhanh trên máy yếu.
    # Ở tốc độ x20, một tháng dữ liệu đủ phát hơn 1 ngày thật.
    months = sorted({m for m in ((start_utc - pd.Timedelta(days=1)).tz_convert(NY).month,
                                 start_utc.tz_convert(NY).month) if 10 <= m <= 12} or {10})
    df = pd.concat([read_traffic_month(m, ("link_id", "speed", "travel_time", "status", "data_as_of"))
                    for m in months], ignore_index=True)
    df = df.sort_values("event_time").reset_index(drop=True)
    df["travel_time"] = df.travel_time.clip(lower=0).astype("uint32")
    df["speed"] = df.speed.astype("float32")
    return df[["link_id", "event_time", "speed", "travel_time", "is_valid"]]


def profile(client) -> pd.DataFrame:
    return client.query_df("""
        SELECT link_id, toDayOfWeek(toTimeZone(hour_utc, 'America/New_York')) AS dow,
               toHour(toTimeZone(hour_utc, 'America/New_York')) AS hour,
               sum(sum_speed) / sum(n_valid) AS hist, stddevPop(sum_speed / n_valid) AS sd
        FROM traffic.hourly GROUP BY link_id, dow, hour
    """).set_index(["link_id", "dow", "hour"])


def write_forecast(client, clock: pd.Timestamp, prof: pd.DataFrame):
    cur = client.query_df("""
        SELECT link_id, argMax(speed, event_time) AS speed
        FROM traffic.reading WHERE is_valid = 1 AND event_time > {c:DateTime} - INTERVAL 15 MINUTE
        GROUP BY link_id
    """, parameters={"c": clock.to_pydatetime()})
    if cur.empty:
        return 0
    issued = clock.floor("min")
    parts = []
    for h, alpha in HORIZONS.items():
        target = issued + pd.Timedelta(minutes=h)
        local = target.tz_convert(NY)
        key = pd.MultiIndex.from_arrays([cur.link_id, np.full(len(cur), local.isoweekday()),
                                         np.full(len(cur), local.hour)])
        p = prof.reindex(key)
        hist = p["hist"].to_numpy()
        pred = np.where(np.isnan(hist), cur.speed, alpha * cur.speed + (1 - alpha) * hist)
        width = 1.28 * p["sd"].fillna(5).to_numpy() * (1.3 - alpha)
        parts.append(pd.DataFrame({
            "link_id": cur.link_id, "issued_at": issued, "target_time": target, "horizon_min": np.uint16(h),
            "pred_speed": pred.astype("float32"), "pred_lower": np.maximum(pred - width, 0).astype("float32"),
            "pred_upper": (pred + width).astype("float32"),
            "model_name": "baseline_seasonal_blend", "model_version": "demo-v0"}))
    fc = pd.concat(parts, ignore_index=True)
    client.insert_df("traffic.forecast", fc)
    return len(fc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-10-07 05:00", help="giờ New York bắt đầu phát (trong 10-12/2025)")
    ap.add_argument("--speed", type=float, default=20.0, help="1 giây thật = N giây dữ liệu")
    ap.add_argument("--tick", type=float, default=3.0)
    ap.add_argument("--no-forecast", action="store_true")
    args = ap.parse_args()

    client = ch_client()
    apply_schema(client)
    start = pd.Timestamp(args.start).tz_localize(NY).tz_convert("UTC")
    df = load_months(start)
    prof = profile(client)
    client.command("TRUNCATE TABLE traffic.reading")
    client.command("TRUNCATE TABLE traffic.forecast")

    # nạp sẵn 24h trước mốc bắt đầu để biểu đồ 24h có dữ liệu ngay
    sent = int(df.event_time.searchsorted(start - pd.Timedelta(days=1)))
    print(f"Replay từ {start.tz_convert(NY)} (New York), x{args.speed}, {len(df) - sent:,} bản ghi khả dụng")
    t0, last_slot = time.time(), None
    while True:
        clock = start + timedelta(seconds=(time.time() - t0) * args.speed)
        upto = int(df.event_time.searchsorted(clock, side="right"))
        if upto > sent:
            client.insert_df("traffic.reading", df.iloc[sent:upto])
            print(f"[{clock.tz_convert(NY):%Y-%m-%d %H:%M}] +{upto - sent:,} bản ghi")
            sent = upto
        if sent >= len(df):
            print("Đã phát hết dữ liệu đã nạp. Đổi DEMO_START sang tháng khác để phát tiếp.")
            break
        slot = clock.floor("5min")
        if not args.no_forecast and slot != last_slot:
            write_forecast(client, clock, prof)
            last_slot = slot
        time.sleep(args.tick)


if __name__ == "__main__":
    main()
