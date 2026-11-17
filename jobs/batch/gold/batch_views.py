"""Batch View 5 phút + 1 giờ (Gold, Iceberg) từ silver.traffic_cleaned. Chỉ Batch Layer ghi các bảng này.

Grain: gold.traffic_5min_batch (link_id, window_start_utc); gold.traffic_hourly (link_id, hour_utc).
Đơn vị recompute = NGÀY UTC. Mỗi bảng ghi bằng MỘT commit `overwrite(<khoảng ngày>)`: thay nguyên tử toàn bộ dữ liệu của
các ngày đó (kể cả ngày không còn dòng nào) -> chạy lại không sinh trùng, không để sót dữ liệu cũ.
Ghi xong KHÔNG làm Trino thấy ngay: Trino đọc Batch View qua tag `published`, chỉ lambda_handoff.py dời tag sau khi kiểm chứng.
Quy tắc chất lượng/DST dùng chung với Speed Layer (common/quality_rules.py, common/time_rules.py, common/aggregations.py):
silver.traffic_cleaned đã loại giờ DST mơ hồ (bản ghi nằm ở silver.traffic_quarantine).
Roll-up giờ chỉ cộng thành phần (n_*, sum_*), không AVG(AVG).
Chạy:
  spark-submit /opt/spark/jobs/batch/gold/batch_views.py --start-day 2025-10-01 --end-day 2025-10-07 [--only 5min|hourly]
"""
import argparse
import datetime as dt
import json
import uuid

from pyspark.sql import DataFrame, SparkSession, functions as F

from common.aggregations import aggregate_5min, rollup_hourly, with_avg
from common.config import GOLD_TRAFFIC_5MIN, GOLD_TRAFFIC_HOURLY, SILVER_TRAFFIC_CLEANED
from common.logger import get_logger
from common.quality_rules import with_quality
from common.schemas import GOLD_TRAFFIC_5MIN_FIELDS, GOLD_TRAFFIC_HOURLY_FIELDS
from common.spark_session import build_spark

log = get_logger("batch_views")


def conform(df: DataFrame, fields: dict) -> DataFrame:
    return df.select([F.col(c).cast(t).alias(c) for c, t in fields.items()])


def utc_range(start: dt.date, end: dt.date) -> tuple[dt.datetime, dt.datetime]:
    return dt.datetime.combine(start, dt.time()), dt.datetime.combine(end + dt.timedelta(days=1), dt.time())


def silver_for_utc_days(spark: SparkSession, start: dt.date, end: dt.date) -> DataFrame:
    lo, hi = utc_range(start, end)
    # Silver phân vùng theo tháng của ngày New York -> lọc rộng ±1 ngày rồi lọc chính xác theo UTC
    return (spark.table(SILVER_TRAFFIC_CLEANED)
            .filter(F.col("date").between(F.lit(start - dt.timedelta(days=1)), F.lit(end + dt.timedelta(days=1))))
            .filter((F.col("traffic_utc") >= F.lit(lo)) & (F.col("traffic_utc") < F.lit(hi))))


def current_snapshot(spark: SparkSession, table: str) -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.history WHERE is_current_ancestor "
                     f"ORDER BY made_current_at DESC LIMIT 1").collect()
    return int(rows[0]["snapshot_id"]) if rows else None


def build(spark: SparkSession, start: dt.date, end: dt.date, batch_run_id: str):
    silver = with_quality(silver_for_utc_days(spark, start, end), "speed", "travel_time")
    agg5 = with_avg(aggregate_5min(silver.withColumnRenamed("traffic_utc", "event_time_utc")))
    five = (agg5.withColumnRenamed("window_start", "window_start_utc").withColumnRenamed("window_end", "window_end_utc")
                .withColumn("quality_flag", F.when(F.col("n_valid") == 0, "NO_VALID_DATA").otherwise("OK"))
                .withColumn("batch_run_id", F.lit(batch_run_id)).withColumn("processed_at", F.current_timestamp()))
    hourly = (with_avg(rollup_hourly(agg5))
              .withColumn("batch_run_id", F.lit(batch_run_id)).withColumn("processed_at", F.current_timestamp()))
    return silver, conform(five, GOLD_TRAFFIC_5MIN_FIELDS), conform(hourly, GOLD_TRAFFIC_HOURLY_FIELDS)


def run(spark: SparkSession, start: dt.date, end: dt.date, batch_run_id: str | None = None, only: str | None = None) -> dict:
    """Ghi lại các ngày UTC [start, end]. Trả về số liệu + snapshot_id mới của từng bảng (để handoff kiểm chứng và gắn tag)."""
    batch_run_id = batch_run_id or f"batch-{uuid.uuid4().hex[:12]}"
    lo, hi = utc_range(start, end)
    silver, five, hourly = build(spark, start, end, batch_run_id)
    five, hourly = five.persist(), hourly.persist()
    stats = {"batch_run_id": batch_run_id, "start_day": str(start), "end_day": str(end),
             "silver_rows": silver.count(), "windows_5min": five.count(), "hours": hourly.count()}
    in_range = lambda c: (F.col(c) >= F.lit(lo)) & (F.col(c) < F.lit(hi))
    if only in (None, "5min"):
        five.writeTo(GOLD_TRAFFIC_5MIN).overwrite(in_range("window_start_utc"))
        stats["snapshot_5min"] = current_snapshot(spark, GOLD_TRAFFIC_5MIN)
    if only in (None, "hourly"):
        hourly.writeTo(GOLD_TRAFFIC_HOURLY).overwrite(in_range("hour_utc"))
        stats["snapshot_hourly"] = current_snapshot(spark, GOLD_TRAFFIC_HOURLY)
    five.unpersist(); hourly.unpersist()
    log.info("[batch_views] %s", json.dumps(stats, default=str))
    return stats


def main():
    ap = argparse.ArgumentParser(description="Batch View 5 phút / 1 giờ -> Iceberg Gold")
    ap.add_argument("--start-day", required=True, help="ngày UTC YYYY-MM-DD")
    ap.add_argument("--end-day", help="ngày UTC, mặc định = start-day")
    ap.add_argument("--only", choices=["5min", "hourly"], help="chỉ ghi một bảng (Airflow tách 2 task)")
    ap.add_argument("--batch-run-id")
    args = ap.parse_args()
    start = dt.date.fromisoformat(args.start_day)
    end = dt.date.fromisoformat(args.end_day or args.start_day)
    spark = build_spark("batch_views")
    run(spark, start, end, args.batch_run_id, args.only)
    spark.stop()


if __name__ == "__main__":
    main()
