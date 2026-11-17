"""Kiểm chứng Serving Layer trên Trino sau Lambda handoff (Airflow task verify). Python thuần + REST Trino.
  1. tag 'published' của 2 bảng Gold trỏ đúng snapshot ghi trong dòng PUBLISHED mới nhất
  2. v_traffic_5min_unified không trùng grain quanh T_batch (±1 ngày)
  3. mỗi cửa sổ trong [T_batch - 1 ngày, T_batch) lấy từ batch, từ T_batch trở đi lấy từ speed (không có window hai nguồn)
  4. v_traffic_hourly_unified: tổng n_records quanh T_batch = tổng 5 phút hợp nhất cùng khoảng
Thoát 1 nếu không đạt. Chạy: python jobs/batch/gold/verify_serving.py [--expect-t-batch 2025-12-02T10:00:00]
"""
import argparse
import json
import sys

from common.trino_http import query


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expect-t-batch", help="nếu có: T_batch hiện hành phải >= giá trị này")
    args = ap.parse_args()
    st = query("SELECT CAST(t_batch AS varchar), snapshot_5min = tag_snapshot_5min, snapshot_hourly = tag_snapshot_hourly, "
               "last_status FROM lakehouse.serving.v_lambda_status")[0]
    t = "lakehouse.serving"
    tb = "(SELECT t_batch FROM lakehouse.serving.v_lambda_t_batch)"
    around = f"window_start_utc >= {tb} - INTERVAL '1' DAY AND window_start_utc < {tb} + INTERVAL '1' DAY"
    dup = query(f"SELECT count(*) - count(DISTINCT (link_id, window_start_utc)) FROM {t}.v_traffic_5min_unified WHERE {around}")[0][0]
    wrong = query(f"SELECT count_if((layer = 'batch' AND window_start_utc >= {tb}) OR (layer = 'speed' AND window_start_utc < {tb})) "
                  f"FROM {t}.v_traffic_5min_unified WHERE {around}")[0][0]
    h = query(f"SELECT coalesce(sum(n_records), 0) FROM {t}.v_traffic_hourly_unified "
              f"WHERE hour_utc >= date_trunc('hour', {tb}) - INTERVAL '1' DAY AND hour_utc < date_trunc('hour', {tb}) + INTERVAL '1' DAY")[0][0]
    f5 = query(f"SELECT coalesce(sum(n_records), 0) FROM {t}.v_traffic_5min_unified "
               f"WHERE window_start_utc >= date_trunc('hour', {tb}) - INTERVAL '1' DAY "
               f"AND window_start_utc < date_trunc('hour', {tb}) + INTERVAL '1' DAY")[0][0]
    res = {"t_batch": st[0], "tag_5min_ok": st[1] in (True, None), "tag_hourly_ok": st[2] in (True, None), "last_status": st[3],
           "duplicate_grain": dup, "wrong_layer": wrong, "hourly_sum": h, "fivemin_sum": f5}
    res["ok"] = res["tag_5min_ok"] and res["tag_hourly_ok"] and dup == 0 and wrong == 0 and h == f5
    if args.expect_t_batch:
        res["t_batch_reached"] = st[0][:19].replace(" ", "T") >= args.expect_t_batch[:19]
        res["ok"] = res["ok"] and res["t_batch_reached"]
    print(json.dumps(res, default=str))
    sys.exit(0 if res["ok"] else 1)


if __name__ == "__main__":
    main()
