"""Batch View 5 phút + 1 giờ (Gold) từ silver.traffic_cleaned, đẩy sang ClickHouse traffic_5min_batch / traffic_hourly.

Grain: (link_id, window_start UTC) và (link_id, hour_utc). Xử lý theo NGÀY UTC (khớp phân vùng Iceberg/ClickHouse):
  - Iceberg: overwritePartitions() -> chạy lại không trùng.
  - ClickHouse: ghi bảng *_staging, kiểm tra số dòng, rồi ALTER TABLE ... REPLACE PARTITION (nguyên tử theo ngày).
Quy tắc chất lượng/DST dùng chung với Speed Layer (common/quality_rules.py, common/time_rules.py):
  silver.traffic_cleaned đã loại giờ DST mơ hồ (các bản ghi đó nằm ở silver.traffic_quarantine và được đếm vào
  batch_run_stats.n_dst_excluded).
Chạy:
  spark-submit /opt/spark/jobs/batch/gold/batch_views.py --start-day 2025-10-01 --end-day 2025-10-07
"""
import argparse
import datetime as dt
import json
import uuid

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, functions as F

from common.aggregations import aggregate_5min, rollup_hourly, with_avg
from common.clickhouse_io import get_client, insert_df
from common.config import (GOLD_TRAFFIC_5MIN, GOLD_TRAFFIC_HOURLY, SILVER_TRAFFIC_CLEANED, SILVER_TRAFFIC_QUARANTINE)
from common.quality_rules import traffic_state_col, with_quality
from common.schemas import GOLD_TRAFFIC_5MIN_FIELDS, GOLD_TRAFFIC_HOURLY_FIELDS
from common.spark_session import build_spark


def conform(df: DataFrame, fields: dict) -> DataFrame:
    return df.select([F.col(c).cast(t).alias(c) for c, t in fields.items()])


def silver_for_utc_days(spark: SparkSession, start: dt.date, end: dt.date) -> DataFrame:
    lo, hi = dt.datetime.combine(start, dt.time()), dt.datetime.combine(end + dt.timedelta(days=1), dt.time())
    # Silver phân vùng theo ngày New York -> lọc rộng ±1 ngày rồi lọc chính xác theo UTC
    return (spark.table(SILVER_TRAFFIC_CLEANED)
            .filter(F.col("date").between(F.lit(start - dt.timedelta(days=1)), F.lit(end + dt.timedelta(days=1))))
            .filter((F.col("traffic_utc") >= F.lit(lo)) & (F.col("traffic_utc") < F.lit(hi))))


def baseline_df(spark: SparkSession) -> DataFrame:
    try:
        rows = get_client().query("SELECT link_id, free_flow_speed FROM link_dim FINAL").result_rows
    except Exception:
        rows = []
    return spark.createDataFrame(pd.DataFrame(rows, columns=["link_id", "free_flow_speed"]), "link_id long, free_flow_speed double")


def _push_days(client, table: str, ts_col: str, pdf: pd.DataFrame, days: list[str]) -> dict:
    """Thay từng phân vùng ngày một cách nguyên tử qua bảng staging. Trả về {ngày: số dòng}."""
    staging = f"{table}_staging"
    out = {}
    key = pdf[ts_col].dt.strftime("%Y%m%d") if len(pdf) else pd.Series(dtype=str)
    for d in days:
        part = pdf[key == d]
        if not len(part):
            # Ngày không có dòng Silver: kết quả recompute là rỗng. ClickHouse mới từ chối REPLACE PARTITION từ nguồn rỗng
            # -> DROP thẳng phân vùng bảng chính (dữ liệu dẫn xuất). lambda_handoff chặn publish nếu realtime có cửa sổ
            # mà batch không có (rt_windows_without_batch), nên Silver thiếu ngày không thể làm mất dữ liệu trên view hợp nhất.
            client.command(f"ALTER TABLE {table} DROP PARTITION {d}")
            out[d] = 0
            continue
        client.command(f"ALTER TABLE {staging} DROP PARTITION {d}")
        insert_df(client, staging, part)
        n = client.query(f"SELECT count() FROM {staging} WHERE toYYYYMMDD({ts_col}) = {d}").result_rows[0][0]
        if n != len(part):
            raise RuntimeError(f"{staging} ngày {d}: ghi {n} dòng, kỳ vọng {len(part)} -> dừng, không thay bảng chính")
        client.command(f"ALTER TABLE {table} REPLACE PARTITION {d} FROM {staging}")
        client.command(f"ALTER TABLE {staging} DROP PARTITION {d}")
        out[d] = int(n)
    return out


def run(spark: SparkSession, start: dt.date, end: dt.date, batch_run_id: str | None = None, push_clickhouse: bool = True) -> dict:
    batch_run_id = batch_run_id or f"batch5-{uuid.uuid4().hex[:12]}"
    now = F.current_timestamp()
    silver = with_quality(silver_for_utc_days(spark, start, end), "speed", "travel_time").persist()
    agg5 = (aggregate_5min(silver.withColumnRenamed("traffic_utc", "event_time_utc"))
            .withColumn("batch_run_id", F.lit(batch_run_id)).withColumn("computed_ts", now)).persist()
    hourly = (rollup_hourly(agg5).withColumn("batch_run_id", F.lit(batch_run_id)).withColumn("computed_ts", now)).persist()

    # 1) Iceberg Gold (dynamic partition overwrite theo ngày UTC)
    conform(agg5, GOLD_TRAFFIC_5MIN_FIELDS).writeTo(GOLD_TRAFFIC_5MIN).overwritePartitions()
    conform(hourly, GOLD_TRAFFIC_HOURLY_FIELDS).writeTo(GOLD_TRAFFIC_HOURLY).overwritePartitions()

    days = [(start + dt.timedelta(days=i)).strftime("%Y%m%d") for i in range((end - start).days + 1)]
    stats = {"batch_run_id": batch_run_id, "start": str(start), "end": str(end),
             "silver_rows": silver.count(), "windows_5min": agg5.count(), "hours": hourly.count()}
    if push_clickhouse:
        client = get_client()
        base = F.broadcast(baseline_df(spark))
        p5 = (with_avg(agg5).join(base, "link_id", "left")
              .withColumn("traffic_state", traffic_state_col("avg_speed", "free_flow_speed", "n_valid"))
              .withColumn("quality_flag", F.when(F.col("n_valid") == 0, "NO_VALID_DATA").otherwise("OK"))
              .withColumnRenamed("computed_ts", "computed_at").drop("free_flow_speed")).toPandas()
        ph = with_avg(hourly).withColumnRenamed("computed_ts", "computed_at").toPandas()
        for pdf, cols in ((p5, ["window_start", "window_end", "computed_at"]), (ph, ["hour_utc", "computed_at"])):
            for c in cols:
                pdf[c] = pd.to_datetime(pdf[c]).dt.tz_localize("UTC")
        stats["ch_5min_rows_by_day"] = _push_days(client, "traffic_5min_batch", "window_start", p5, days)
        stats["ch_hourly_rows_by_day"] = _push_days(client, "traffic_hourly", "hour_utc", ph, days)

        # Nhật ký từng ngày: số dòng Silver, hợp lệ, số bản ghi DST bị loại khỏi gom theo thời gian
        per_day = (silver.groupBy(F.date_format("traffic_utc", "yyyyMMdd").alias("d"))
                   .agg(F.count("*").alias("n"), F.sum(F.col("is_valid").cast("int")).alias("v")).collect())
        try:
            q = (spark.table(SILVER_TRAFFIC_QUARANTINE)
                 .filter(F.col("reason").startswith("dst_") & F.col("date").between(F.lit(start), F.lit(end)))
                 .groupBy(F.date_format("date", "yyyyMMdd").alias("d")).count().collect())
            dst = {r["d"]: int(r["count"]) for r in q}
        except Exception:
            dst = {}
        win = {r["d"]: int(r["c"]) for r in agg5.groupBy(F.date_format("window_start", "yyyyMMdd").alias("d")).agg(F.count("*").alias("c")).collect()}
        rows = [{"batch_run_id": batch_run_id, "day": dt.datetime.strptime(r["d"], "%Y%m%d").date(), "n_silver_rows": int(r["n"]),
                 "n_valid": int(r["v"] or 0), "n_dst_excluded": dst.get(r["d"], 0), "n_windows": win.get(r["d"], 0),
                 "checks": json.dumps({"ch_rows": stats["ch_5min_rows_by_day"].get(r["d"])}), "computed_at": pd.Timestamp.now(tz="UTC")}
                for r in per_day]
        insert_df(client, "batch_run_stats", pd.DataFrame(rows))
        stats["n_dst_excluded"] = sum(dst.values())
    silver.unpersist(); agg5.unpersist(); hourly.unpersist()
    print("[batch_views] " + json.dumps(stats, default=str))
    return stats


def main():
    ap = argparse.ArgumentParser(description="Batch View 5 phút / 1 giờ")
    ap.add_argument("--start-day", required=True, help="ngày UTC YYYY-MM-DD")
    ap.add_argument("--end-day", help="ngày UTC, mặc định = start-day")
    ap.add_argument("--no-clickhouse", action="store_true")
    args = ap.parse_args()
    start = dt.date.fromisoformat(args.start_day)
    end = dt.date.fromisoformat(args.end_day or args.start_day)
    spark = build_spark("batch_views")
    run(spark, start, end, push_clickhouse=not args.no_clickhouse)
    spark.stop()


if __name__ == "__main__":
    main()
