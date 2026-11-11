"""Lambda handoff: chuyển các cửa sổ 5 phút đã hoàn tất từ Speed Layer sang Batch View, theo mốc T_batch.

  Batch    : window_start <  T_batch   (traffic_5min_batch)
  Realtime : window_start >= T_batch   (traffic_5min_rt)
Quy trình (mỗi bước chỉ chạy khi bước trước thành công; T_batch chỉ tăng ở bước 4):
  1. Recompute Batch View cho các ngày UTC từ ngày của T_batch hiện hành đến ngày chứa T_new - 1s.
  2. Commit (Iceberg overwritePartitions + ClickHouse REPLACE PARTITION, xem batch_views.py).
  3. Kiểm tra trên khoảng [T_old, T_new): đủ dữ liệu so với Silver, không trùng grain, đối chiếu với realtime.
  4. Publish: thêm 1 dòng PUBLISHED vào lambda_handoff (một INSERT -> nguyên tử). Lỗi kiểm tra -> REJECTED, thoát 1.
  5. (tuỳ chọn) Dọn phân vùng realtime đã hoàn toàn nằm trước T_new - giữ lại N ngày.
Crash giữa các bước: T_batch chưa đổi -> view hợp nhất vẫn lấy realtime cho các cửa sổ đó; chạy lại là đủ (idempotent).
Dữ liệu đến muộn sau khi đã chốt: chạy lại batch_views.py cho ngày bị ảnh hưởng (REPLACE PARTITION), T_batch không đổi.

Chạy: spark-submit /opt/spark/jobs/batch/gold/lambda_handoff.py --until auto
      spark-submit /opt/spark/jobs/batch/gold/lambda_handoff.py --until 2025-10-03T00:00:00 --fail-at after_commit   (thử crash)
"""
import argparse
import datetime as dt
import json
import sys
import uuid

import pandas as pd
from pyspark.sql import functions as F

from common import config
from common.clickhouse_io import get_client, insert_df
from common.spark_session import build_spark
from batch.gold import batch_views

UTC = dt.timezone.utc


def floor5(t: dt.datetime) -> dt.datetime:
    return t.replace(minute=t.minute - t.minute % 5, second=0, microsecond=0)


def current_t_batch(client) -> dt.datetime:
    t = client.query("SELECT t_batch FROM v_lambda_t_batch").result_rows[0][0]
    return t.replace(tzinfo=None) if t else dt.datetime(1970, 1, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--until", default="auto", help="T_batch mới (UTC, làm tròn xuống 5 phút) hoặc 'auto'")
    ap.add_argument("--lateness-minutes", type=int, default=None, help="auto: lùi khỏi event-time lớn nhất của Silver")
    ap.add_argument("--from-day", help="ép ngày UTC bắt đầu recompute (vd backfill)")
    ap.add_argument("--max-rt-diff-records", type=int, default=0, help="chênh lệch n_records tối đa so với realtime được ghi nhận (chỉ báo cáo)")
    ap.add_argument("--allow-rt-without-batch", action="store_true",
                    help="vẫn publish khi realtime có cửa sổ batch không có (vd đã chủ động xoá Silver)")
    ap.add_argument("--fail-at", choices=["after_commit", "after_verify"], help="giả lập crash để kiểm thử")
    ap.add_argument("--cleanup-rt-keep-days", type=int, default=-1, help=">=0: xoá phân vùng realtime trước ngày(T_new) - N")
    args = ap.parse_args()

    spark = build_spark("lambda_handoff")
    client = get_client()
    t_old = current_t_batch(client)
    silver = spark.table(config.SILVER_TRAFFIC_CLEANED)
    if args.until == "auto":
        max_ts = silver.agg(F.max("traffic_utc")).collect()[0][0]
        lateness = args.lateness_minutes if args.lateness_minutes is not None else int(config.SPEED_WATERMARK.split()[0])
        t_new = floor5(max_ts - dt.timedelta(minutes=lateness))
    else:
        t_new = floor5(dt.datetime.fromisoformat(args.until))
    print(f"[handoff] T_old={t_old} T_new={t_new}")
    if t_new <= t_old:
        print("[handoff] không có cửa sổ mới hoàn tất -> giữ nguyên T_batch"); return

    if args.from_day:
        start_day = dt.date.fromisoformat(args.from_day)
    else:  # lần đầu (T_old = 1970): bắt đầu từ ngày nhỏ nhất có trong Silver, không quét cả năm
        min_ts = silver.agg(F.min("traffic_utc")).collect()[0][0]
        start_day = max(t_old.date(), min_ts.date()) if min_ts else t_old.date()
    end_day = (t_new - dt.timedelta(seconds=1)).date()
    run_id = f"handoff-{uuid.uuid4().hex[:12]}"

    # 1-2. Recompute + commit Batch View
    stats = batch_views.run(spark, start_day, end_day, batch_run_id=run_id)
    if args.fail_at == "after_commit":
        raise SystemExit("[handoff] giả lập crash sau commit, trước kiểm tra/publish")

    # 3. Kiểm tra trên [T_old, T_new)
    lo, hi = max(t_old, dt.datetime.combine(start_day, dt.time())), t_new
    silver_n = silver.filter((F.col("traffic_utc") >= F.lit(lo)) & (F.col("traffic_utc") < F.lit(hi))).count()
    rng = f"window_start >= toDateTime('{lo:%Y-%m-%d %H:%M:%S}', 'UTC') AND window_start < toDateTime('{hi:%Y-%m-%d %H:%M:%S}', 'UTC')"
    b_n, b_rows, b_keys = client.query(f"SELECT sum(n_records), count(), uniqExact(link_id, window_start) FROM traffic_5min_batch FINAL WHERE {rng}").result_rows[0]
    cmp = client.query(f"""
        SELECT count() AS windows_both,
               sum(abs(toInt64(b.n_records) - toInt64(r.n_records))) AS diff_records,
               max(abs(if(b.n_valid > 0, b.sum_speed / b.n_valid, 0) - if(r.n_valid > 0, r.sum_speed / r.n_valid, 0))) AS max_avg_diff
        FROM (SELECT * FROM traffic_5min_batch FINAL WHERE {rng}) b
        INNER JOIN (SELECT * FROM traffic_5min_rt FINAL WHERE {rng}) r USING (link_id, window_start)""").result_rows[0]
    rt_only = client.query(f"""SELECT count() FROM (SELECT link_id, window_start FROM traffic_5min_rt FINAL WHERE {rng}) r
        LEFT ANTI JOIN (SELECT link_id, window_start FROM traffic_5min_batch FINAL WHERE {rng}) b USING (link_id, window_start)""").result_rows[0][0]
    checks = {"range": [str(lo), str(hi)], "silver_rows": silver_n, "batch_sum_n_records": int(b_n or 0),
              "complete": int(b_n or 0) == silver_n, "batch_rows": b_rows, "unique_grain": b_rows == b_keys,
              "rt_windows_compared": cmp[0], "rt_diff_records": int(cmp[1] or 0), "rt_max_avg_speed_diff": float(cmp[2] or 0),
              "rt_windows_without_batch": rt_only, "batch_stats": stats}
    # Batch phải phủ mọi cửa sổ realtime đã thấy (Silver ⊇ dữ liệu speed layer). Thiếu -> Silver chưa đủ, không publish.
    checks["covers_realtime"] = rt_only == 0 or args.allow_rt_without_batch
    ok = checks["complete"] and checks["unique_grain"] and checks["covers_realtime"]
    print("[handoff] checks " + json.dumps(checks, default=str))
    if args.fail_at == "after_verify":
        raise SystemExit("[handoff] giả lập crash sau kiểm tra, trước publish")

    # 4. Publish (1 INSERT)
    row = pd.DataFrame([{"t_batch": pd.Timestamp(t_new, tz="UTC"), "batch_run_id": run_id, "status": "PUBLISHED" if ok else "REJECTED",
                         "checks": json.dumps(checks, default=str), "published_at": pd.Timestamp.now(tz="UTC")}])
    insert_df(client, "lambda_handoff", row)
    if not ok:
        print("[handoff] KIỂM TRA KHÔNG ĐẠT -> REJECTED, T_batch giữ nguyên"); sys.exit(1)
    print(f"[handoff] PUBLISHED T_batch = {t_new}")

    # 5. Dọn realtime cũ (chỉ phân vùng NGÀY nằm hoàn toàn trước T_new - keep_days)
    if args.cleanup_rt_keep_days >= 0:
        cutoff = (t_new.date() - dt.timedelta(days=args.cleanup_rt_keep_days)).strftime("%Y%m%d")
        parts = [r[0] for r in client.query(f"SELECT DISTINCT partition FROM system.parts WHERE database = '{config.CLICKHOUSE_SERVING_DB}' "
                                            f"AND table = 'traffic_5min_rt' AND active AND partition < '{cutoff}'").result_rows]
        for p in parts:
            client.command(f"ALTER TABLE traffic_5min_rt DROP PARTITION {p}")
        print(f"[handoff] đã dọn {len(parts)} phân vùng realtime trước {cutoff}")
    spark.stop()


if __name__ == "__main__":
    main()
