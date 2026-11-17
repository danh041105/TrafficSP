"""Kiểm tra chất lượng dữ liệu trước Lambda handoff (Airflow task dq_checks). Đọc nhánh main của Gold (bản vừa ghi,
chưa công bố) + Silver cho các ngày UTC [start, end]. Thoát 1 nếu có kiểm tra bắt buộc không đạt -> handoff không chạy.

Bắt buộc:
  silver_no_null_key        : Silver không có link_id / traffic_utc NULL
  gold_unique_grain         : Gold 5 phút không trùng (link_id, window_start_utc); 1 giờ không trùng (link_id, hour_utc)
  gold_complete             : tổng n_records Gold 5 phút = số dòng Silver cùng khoảng
  gold_counts_consistent    : n_valid + n_zero + n_invalid_other <= n_records, n_valid >= 0
  gold_avg_in_range         : avg_speed = sum_speed / n_valid, nằm trong (0, 100] và min <= avg <= max
  hourly_equals_5min        : tổng thành phần theo giờ = tổng các cửa sổ 5 phút trong giờ đó
Báo cáo (không chặn): số bản ghi quarantine (DST/thiếu khoá), tỷ lệ cửa sổ NO_VALID_DATA.
Chạy: spark-submit .../dq_checks.py --start-day 2025-12-01 --end-day 2025-12-02
"""
import argparse
import datetime as dt
import json
import sys

from pyspark.sql import functions as F

from common import config
from common.logger import get_logger
from common.spark_session import build_spark
from batch.gold.batch_views import silver_for_utc_days, utc_range

log = get_logger("dq_checks")


def run(spark, start: dt.date, end: dt.date) -> dict:
    lo, hi = utc_range(start, end)
    rng = lambda c: (F.col(c) >= F.lit(lo)) & (F.col(c) < F.lit(hi))
    silver = silver_for_utc_days(spark, start, end)
    g5 = spark.table(config.GOLD_TRAFFIC_5MIN).filter(rng("window_start_utc"))
    gh = spark.table(config.GOLD_TRAFFIC_HOURLY).filter(rng("hour_utc"))
    s = silver.agg(F.count("*").alias("n"), F.sum((F.col("link_id").isNull() | F.col("traffic_utc").isNull()).cast("int")).alias("null_key")).collect()[0]
    a5 = g5.agg(F.count("*").alias("rows"), F.countDistinct("link_id", "window_start_utc").alias("keys"),
                F.coalesce(F.sum("n_records"), F.lit(0)).alias("n"),
                F.sum(((F.col("n_valid") + F.col("n_zero") + F.col("n_invalid_other")) > F.col("n_records")).cast("int")).alias("bad_counts"),
                F.sum((F.col("n_valid") < 0).cast("int")).alias("neg"),
                F.sum(((F.col("n_valid") > 0) & (
                    (F.abs(F.col("avg_speed") - F.col("sum_speed") / F.col("n_valid")) > 1e-9) | (F.col("avg_speed") <= 0)
                    | (F.col("avg_speed") > 100) | (F.col("avg_speed") < F.col("min_speed") - 1e-9)
                    | (F.col("avg_speed") > F.col("max_speed") + 1e-9))).cast("int")).alias("bad_avg"),
                F.sum((F.col("n_valid") == 0).cast("int")).alias("no_valid")).collect()[0]
    ah = gh.agg(F.count("*").alias("rows"), F.countDistinct("link_id", "hour_utc").alias("keys")).collect()[0]
    roll = (g5.groupBy("link_id", F.date_trunc("hour", "window_start_utc").alias("hour_utc"))
              .agg(F.sum("n_records").alias("n5"), F.sum("n_valid").alias("v5"), F.sum("sum_speed").alias("s5")))
    mism = (roll.join(gh, ["link_id", "hour_utc"], "full_outer")
                .filter(F.col("n5").isNull() | F.col("n_records").isNull() | (F.col("n5") != F.col("n_records"))
                        | (F.col("v5") != F.col("n_valid")) | (F.abs(F.col("s5") - F.col("sum_speed")) > 1e-6))).count()
    try:
        quarantine = spark.table(config.SILVER_TRAFFIC_QUARANTINE).filter(
            F.col("date").between(F.lit(start - dt.timedelta(days=1)), F.lit(end))).groupBy("reason").count().collect()
        quarantine = {r["reason"]: r["count"] for r in quarantine}
    except Exception:
        quarantine = {}
    checks = {
        "silver_no_null_key": s["null_key"] in (0, None),
        "gold_unique_grain": a5["rows"] == a5["keys"] and ah["rows"] == ah["keys"],
        "gold_complete": int(a5["n"]) == s["n"],
        "gold_counts_consistent": (a5["bad_counts"] or 0) == 0 and (a5["neg"] or 0) == 0,
        "gold_avg_in_range": (a5["bad_avg"] or 0) == 0,
        "hourly_equals_5min": mism == 0,
    }
    report = {"range": [str(lo), str(hi)], "silver_rows": s["n"], "gold_5min_rows": a5["rows"], "gold_hourly_rows": ah["rows"],
              "hourly_mismatch_rows": mism, "no_valid_windows": a5["no_valid"], "quarantine": quarantine,
              "checks": checks, "ok": all(checks.values())}
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-day", required=True)
    ap.add_argument("--end-day")
    args = ap.parse_args()
    spark = build_spark("dq_checks")
    r = run(spark, dt.date.fromisoformat(args.start_day), dt.date.fromisoformat(args.end_day or args.start_day))
    log.info("[dq] %s", json.dumps(r, default=str))
    spark.stop()
    sys.exit(0 if r["ok"] else 1)


if __name__ == "__main__":
    main()
