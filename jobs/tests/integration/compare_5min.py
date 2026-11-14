# Đối chiếu bảng/view 5 phút (speed.traffic_5min_rt, gold.traffic_5min_batch, serving.v_traffic_5min_unified - đọc qua
# Trino) với tính toán ĐỘC LẬP bằng DuckDB trực tiếp từ file Parquet gốc, cùng quy tắc (0 < speed <= 100, travel_time > 0;
# bỏ giờ DST mơ hồ).
# Chạy trên máy host (cần duckdb, pandas):
#   python jobs/tests/integration/compare_5min.py --csv logs/u.csv --start 2025-12-02T05:00 --end 2025-12-02T10:45 --months 12
# CSV lấy bằng:
#   docker exec traffic-trino trino --output-format CSV_HEADER_UNQUOTED --execute "SELECT link_id,
#       CAST(to_unixtime(window_start_utc) AS bigint) ws, n_records, n_valid, n_zero, n_invalid_other, sum_speed,
#       sum_travel_time, min_speed, max_speed FROM lakehouse.serving.v_traffic_5min_unified
#       WHERE window_start_utc >= TIMESTAMP '2025-12-02 05:00 UTC'" > logs/u.csv
import argparse
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
COLS = ["n_records", "n_valid", "n_zero", "n_invalid_other", "sum_travel_time"]


def reference(start: str, end: str, months: list[str]) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    files = ",".join(f"'{(ROOT / 'data/raw/nyc_dot' / f'2025-{m}.parquet').as_posix()}'" for m in months)
    return con.execute(f"""
        WITH r AS (
          SELECT CAST(link_id AS BIGINT) link_id, speed, travel_time, CAST(data_as_of AS TIMESTAMP) loc
          FROM read_parquet([{files}])),
        f AS (  -- giờ DST mơ hồ 2025-11-02 01:xx: không có UTC xác định -> loại khỏi gom theo thời gian
          SELECT *, timezone('America/New_York', loc) ev FROM r
          WHERE NOT (CAST(loc AS DATE) = DATE '2025-11-02' AND hour(loc) = 1)),
        v AS (SELECT *, (speed > 0 AND speed <= 100 AND travel_time > 0) ok FROM f
              WHERE ev >= TIMESTAMPTZ '{start}:00+00' AND ev < TIMESTAMPTZ '{end}:00+00'),
        d AS (SELECT DISTINCT * FROM v)                        -- khử trùng theo toàn bộ giá trị (nguồn không trùng)
        SELECT link_id, CAST(floor(epoch(ev) / 300) * 300 AS BIGINT) ws, count(*) n_records,
               count(*) FILTER (WHERE ok) n_valid, count(*) FILTER (WHERE speed = 0) n_zero,
               count(*) FILTER (WHERE NOT ok AND speed <> 0) n_invalid_other,
               coalesce(sum(speed) FILTER (WHERE ok), 0) sum_speed, coalesce(sum(travel_time) FILTER (WHERE ok), 0) sum_travel_time,
               min(speed) FILTER (WHERE ok) min_speed, max(speed) FILTER (WHERE ok) max_speed
        FROM d GROUP BY 1, 2""").df()


def compare(csv: str, start: str, end: str, months: list[str]) -> dict:
    ref = reference(start, end, months)
    got = pd.read_csv(csv, na_values=["\\N"])
    s, e = pd.Timestamp(start, tz="UTC").timestamp(), pd.Timestamp(end, tz="UTC").timestamp()
    got = got[(got.ws >= s) & (got.ws < e)]
    m = ref.merge(got, on=["link_id", "ws"], how="outer", suffixes=("_ref", "_got"), indicator=True)
    both = m[m._merge == "both"]
    res = {"range": [start, end], "ref_windows": len(ref), "got_windows": len(got),
           "only_ref": int((m._merge == "left_only").sum()), "only_got": int((m._merge == "right_only").sum()),
           "ref_records": int(ref.n_records.sum()), "got_records": int(got.n_records.sum())}
    for c in COLS:
        res[f"mismatch_{c}"] = int((both[f"{c}_ref"] != both[f"{c}_got"]).sum())
    res["max_abs_diff_sum_speed"] = float((both.sum_speed_ref - both.sum_speed_got).abs().max() or 0)
    for c in ("min_speed", "max_speed"):
        res[f"mismatch_{c}"] = int(((both[f"{c}_ref"].fillna(-1) - both[f"{c}_got"].fillna(-1)).abs() > 1e-9).sum())
    res["identical"] = (res["only_ref"] == 0 and res["only_got"] == 0 and res["max_abs_diff_sum_speed"] < 1e-6
                        and all(v == 0 for k, v in res.items() if k.startswith("mismatch_")))
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--start", required=True, help="UTC, vd 2025-10-01T04:00")
    ap.add_argument("--end", required=True)
    ap.add_argument("--months", default="10", help="file nguồn, vd 10,11")
    a = ap.parse_args()
    r = compare(a.csv, a.start, a.end, a.months.split(","))
    print(json.dumps(r, indent=1))
    sys.exit(0 if r["identical"] else 1)
