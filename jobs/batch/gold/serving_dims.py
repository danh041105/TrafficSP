"""Chuẩn bị dữ liệu phục vụ cho Speed/Query Layer:
  1. baseline : free-flow theo từng link (D-18) -> gold.link_baseline
                p85 tốc độ HỢP LỆ, lúc khô (mưa < 0,1 mm và không có tuyết trong KHOẢNG giờ [H, H+1): đọc bản ghi
                weather mốc H+1, quy tắc nhánh khai), 22:00-05:59 giờ New York, cần >= 100 mẫu.
                Mặc định chỉ dùng lịch sử GĐ1 (01/01-30/09/2025) để không "nhìn trước" dữ liệu GĐ2 đang phát lại.
  2. link_dim : silver.link_info (vị trí, ô thời tiết - mapping của dev) + baseline -> ClickHouse link_dim
  3. weather  : silver.weather_cleaned -> ClickHouse weather_hourly (source='batch') để ghép thời tiết cho Batch View
Chạy: spark-submit /opt/spark/jobs/batch/gold/serving_dims.py --steps baseline,link_dim,weather
"""
import argparse
import datetime as dt

import pandas as pd
from pyspark.sql import functions as F

from common import config
from common.clickhouse_io import get_client, insert_df
from common.quality_rules import with_quality
from common.schemas import GOLD_LINK_BASELINE_FIELDS
from common.spark_session import build_spark
from common.time_rules import TRAFFIC_TIMEZONE

STEPS = ["baseline", "link_dim", "weather"]


def run_baseline(spark, hist_start: str, hist_end: str, version: str):
    t = with_quality(spark.table(config.SILVER_TRAFFIC_CLEANED)
                     .filter(F.col("date").between(F.lit(hist_start).cast("date"), F.lit(hist_end).cast("date"))), "speed", "travel_time")
    t = (t.filter("is_valid")
          .withColumn("local_hour", F.hour(F.from_utc_timestamp("traffic_utc", TRAFFIC_TIMEZONE)))
          .filter(F.col("local_hour").isin(*config.FREE_FLOW_LOCAL_HOURS))
          .withColumn("weather_end", F.date_trunc("hour", "traffic_utc") + F.expr("INTERVAL 1 HOUR")))
    links = spark.table(config.SILVER_LINK_INFO).select("link_id", "cell_id")
    w = spark.table(config.SILVER_WEATHER_CLEANED).select("cell_id", F.col("weather_utc").alias("weather_end"), "precipitation", "snowfall")
    dry = (t.join(F.broadcast(links), "link_id").join(w, ["cell_id", "weather_end"])
            .filter((F.col("precipitation") < 0.1) & (F.col("snowfall") == 0)))
    b = (dry.groupBy("link_id")
            .agg(F.expr(f"percentile(speed, {config.FREE_FLOW_PERCENTILE})").alias("free_flow_speed"), F.count("*").alias("n_samples"))
            .withColumn("free_flow_speed", F.when(F.col("n_samples") >= config.FREE_FLOW_MIN_SAMPLES, F.col("free_flow_speed")))
            .withColumn("history_start", F.lit(hist_start).cast("date")).withColumn("history_end", F.lit(hist_end).cast("date"))
            .withColumn("baseline_version", F.lit(version)).withColumn("computed_ts", F.current_timestamp()))
    b.select([F.col(c).cast(tp).alias(c) for c, tp in GOLD_LINK_BASELINE_FIELDS.items()]) \
        .writeTo(config.GOLD_LINK_BASELINE).overwrite(F.lit(True))
    n = spark.table(config.GOLD_LINK_BASELINE)
    print(f"[baseline] {n.count()} link, có free-flow: {n.filter('free_flow_speed IS NOT NULL').count()}")


def run_link_dim(spark):
    li = spark.table(config.SILVER_LINK_INFO)
    try:
        base = spark.table(config.GOLD_LINK_BASELINE).select("link_id", "free_flow_speed", "n_samples", "baseline_version")
    except Exception:
        base = spark.createDataFrame([], "link_id long, free_flow_speed double, n_samples long, baseline_version string")
    pdf = (li.join(base, "link_id", "left")
             .select("link_id", "link_name", "borough", "latitude", "longitude", "cell_id", "cell_distance_km",
                     F.lit("dev-median-nearest-v1").alias("mapping_version"), "free_flow_speed",
                     F.coalesce("n_samples", F.lit(0)).cast("int").alias("free_flow_samples"),
                     F.coalesce("baseline_version", F.lit("")).alias("baseline_version"))).toPandas()
    pdf["updated_at"] = pd.Timestamp.now(tz="UTC")
    insert_df(get_client(), "link_dim", pdf)
    print(f"[link_dim] {len(pdf)} link -> ClickHouse")


def run_weather(spark, start: str | None, end: str | None):
    w = spark.table(config.SILVER_WEATHER_CLEANED)
    if start:
        w = w.filter(F.col("weather_utc") >= F.lit(start).cast("timestamp"))
    if end:
        w = w.filter(F.col("weather_utc") < F.lit(end).cast("timestamp"))
    pdf = w.select("cell_id", F.col("weather_utc").alias("time_utc"), "temperature_2m", "relative_humidity_2m", "precipitation",
                   "snowfall", "wind_speed_10m", "wind_gusts_10m", "weather_code", "run_id").toPandas()
    pdf["time_utc"] = pd.to_datetime(pdf["time_utc"]).dt.tz_localize("UTC")
    pdf["source"], pdf["kafka_ts"], pdf["updated_at"] = "batch", pd.NaT, pd.Timestamp.now(tz="UTC")
    client = get_client()
    for i in range(0, len(pdf), 50_000):
        insert_df(client, "weather_hourly", pdf.iloc[i:i + 50_000])
    print(f"[weather] {len(pdf)} dòng -> ClickHouse weather_hourly")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default=",".join(STEPS))
    ap.add_argument("--history-start", default="2025-01-01")
    ap.add_argument("--history-end", default="2025-09-30")
    ap.add_argument("--weather-start")
    ap.add_argument("--weather-end")
    args = ap.parse_args()
    spark = build_spark("serving_dims")
    steps = [s.strip() for s in args.steps.split(",")]
    if "baseline" in steps:
        run_baseline(spark, args.history_start, args.history_end, f"p85-dry-night-{args.history_start}-{args.history_end}")
    if "link_dim" in steps:
        run_link_dim(spark)
    if "weather" in steps:
        run_weather(spark, args.weather_start, args.weather_end)
    spark.stop()


if __name__ == "__main__":
    main()
