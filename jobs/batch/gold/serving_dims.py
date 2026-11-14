"""Free-flow speed theo từng link (D-18) -> gold.link_baseline (Iceberg).
  p85 tốc độ HỢP LỆ, lúc khô (mưa < 0,1 mm và không có tuyết trong KHOẢNG giờ [H, H+1): đọc bản ghi weather mốc H+1,
  quy tắc nhánh khai), 22:00-05:59 giờ New York, cần >= 100 mẫu (ít hơn -> free_flow_speed NULL -> NO_BASELINE).
  Mặc định chỉ dùng lịch sử GĐ1 (01/01-30/09/2025) để không "nhìn trước" dữ liệu GĐ2 đang phát lại.
Link dimension (vị trí, ô thời tiết, free-flow) và thời tiết theo khoảng giờ là VIEW Trino (jobs/serving/trino_views.sql),
không chép dữ liệu sang hệ thống khác.
Chạy: spark-submit /opt/spark/jobs/batch/gold/serving_dims.py [--history-start 2025-01-01 --history-end 2025-09-30]
"""
import argparse

from pyspark.sql import functions as F

from common import config
from common.logger import get_logger
from common.quality_rules import with_quality
from common.schemas import GOLD_LINK_BASELINE_FIELDS
from common.spark_session import build_spark
from common.time_rules import TRAFFIC_TIMEZONE

log = get_logger("serving_dims")


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
    log.info("[baseline] %d link, có free-flow: %d", n.count(), n.filter("free_flow_speed IS NOT NULL").count())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--history-start", default="2025-01-01")
    ap.add_argument("--history-end", default="2025-09-30")
    args = ap.parse_args()
    spark = build_spark("serving_dims")
    run_baseline(spark, args.history_start, args.history_end, f"p85-dry-night-{args.history_start}-{args.history_end}")
    spark.stop()


if __name__ == "__main__":
    main()
