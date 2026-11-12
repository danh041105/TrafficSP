"""Bronze -> Silver: weather_cleaned, link_info, traffic_cleaned và nhật ký ingest_audit.

Ngày truyền vào là ngày theo giờ New York, tính cả hai đầu. Ví dụ:
  spark-submit .../bronze_to_silver.py --start-date 2025-01-01 --end-date 2025-09-30
  spark-submit .../bronze_to_silver.py --start-date 2025-10-05 --steps weather,traffic
"""
import argparse
import uuid
from datetime import date
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, functions as F
from common.config import (BRONZE_TRAFFIC_TABLE, BRONZE_WEATHER_TABLE, SILVER_TRAFFIC_CLEANED, SILVER_WEATHER_CLEANED,
                           SILVER_LINK_INFO, SILVER_INGEST_AUDIT)
from common.schemas import (SILVER_TRAFFIC_FIELDS, SILVER_WEATHER_FIELDS,
                            SILVER_LINK_INFO_FIELDS, SILVER_INGEST_AUDIT_FIELDS)
from common.spark_session import build_spark
from common.logger import get_logger
from common.data_cleaning import (cast_traffic, parsed_traffic_time, drop_null_keys,
                                  drop_dst_repeated_hours, deduplicate, parse_weather_time)
from batch.transformation.link_location import (clean_link_points, add_median_point,
                                                assign_nearest_cell)

STEPS = ["weather", "link_info", "traffic"]
CACHE = StorageLevel.MEMORY_AND_DISK

log = get_logger("bronze_to_silver")

# Chọn đúng cột, đúng thứ tự và đúng kiểu theo schema Silver
def conform(df: DataFrame, fields: dict) -> DataFrame:
    return df.select([F.col(c).cast(t).alias(c) for c, t in fields.items()])

def write_audit(source: str, event_date: Column, raw: DataFrame, before_dedup: DataFrame,
                after_dedup: DataFrame, job_run_id: str) -> None:

    keys = ["event_date", "kafka_partition"]

    def by_key(df):
        return df.withColumn("event_date", event_date).groupBy(*keys)

    stats = by_key(raw).agg(
        F.min("kafka_offset").alias("min_offset"),
        F.max("kafka_offset").alias("max_offset"),
        F.min("ingest_ts").alias("min_ingest_ts"),
        F.max("ingest_ts").alias("max_ingest_ts"),
        F.count("*").alias("n_bronze_rows"),
    )
    n_before = by_key(before_dedup).agg(F.count("*").alias("n_before_dedup"))
    n_after = by_key(after_dedup).agg(F.count("*").alias("n_silver_rows"))

    audit = (stats.join(n_before, keys, "left").join(n_after, keys, "left")
             .fillna(0, ["n_before_dedup", "n_silver_rows"])
             .withColumn("n_duplicates", F.col("n_before_dedup") - F.col("n_silver_rows"))
             .withColumn("n_invalid", F.col("n_bronze_rows") - F.col("n_before_dedup"))
             .withColumn("job_run_id", F.lit(job_run_id))
             .withColumn("source", F.lit(source))
             .withColumn("processed_ts", F.current_timestamp()))

    conform(audit, SILVER_INGEST_AUDIT_FIELDS).writeTo(SILVER_INGEST_AUDIT).append()


def run_weather(spark, job_run_id: str) -> None:
    raw = parse_weather_time(spark.table(BRONZE_WEATHER_TABLE)).persist(CACHE)
    cleaned = drop_null_keys(raw, ["cell_id", "weather_utc"])
    silver = deduplicate(cleaned, ["cell_id", "weather_utc"]).persist(CACHE)
    out = conform(silver.withColumn("processed_ts", F.current_timestamp()), SILVER_WEATHER_FIELDS)
    out.writeTo(SILVER_WEATHER_CLEANED).overwrite(F.lit(True))          # bảng nhỏ: ghi đè toàn bảng
    write_audit("weather", F.to_date("weather_utc"), raw, cleaned, silver, job_run_id)

    log.info("[weather] đã ghi %d dòng", silver.count())
    raw.unpersist()
    silver.unpersist()


def run_link_info(spark) -> None:
    links = cast_traffic(spark.table(BRONZE_TRAFFIC_TABLE))
    links = deduplicate(links, ["link_id"])                      # bản mới nhất của mỗi link
    links = clean_link_points(links)
    links = drop_null_keys(links, ["link_id", "link_points"])
    links = add_median_point(links)

    cells = spark.table(SILVER_WEATHER_CLEANED).select("cell_id", "latitude", "longitude").distinct()
    out = assign_nearest_cell(links, cells).withColumn("processed_ts", F.current_timestamp())
    conform(out, SILVER_LINK_INFO_FIELDS).writeTo(SILVER_LINK_INFO).overwrite(F.lit(True))

    log.info("[link_info] đã ghi %d link", spark.table(SILVER_LINK_INFO).count())


def run_traffic(spark, start: str, end: str, job_run_id: str) -> None:
    # data_as_of là giờ New York nên event_month cũng theo giờ New York: lọc đúng partition Bronze
    bronze = spark.table(BRONZE_TRAFFIC_TABLE).filter(F.col("event_month").between(start[:7], end[:7]))
    raw = (parsed_traffic_time(cast_traffic(bronze))
           .filter(F.col("date").between(F.lit(start).cast("date"), F.lit(end).cast("date")))
           .persist(CACHE))
    cleaned = drop_dst_repeated_hours(drop_null_keys(raw, ["link_id", "traffic_utc"]))
    silver = deduplicate(cleaned, ["link_id", "traffic_utc"]).persist(CACHE)

    # Xóa đúng các ngày đang xử lý rồi ghi lại -> chạy lại bao nhiêu lần cũng không sinh trùng
    spark.sql(f"DELETE FROM {SILVER_TRAFFIC_CLEANED} WHERE date BETWEEN DATE '{start}' AND DATE '{end}'")
    out = conform(silver.withColumn("processed_ts", F.current_timestamp()), SILVER_TRAFFIC_FIELDS)
    out.writeTo(SILVER_TRAFFIC_CLEANED).append()
    write_audit("traffic", F.col("date"), raw, cleaned, silver, job_run_id)

    log.info("[traffic] %s -> %s: đã ghi %d dòng", start, end, silver.count())
    raw.unpersist()
    silver.unpersist()

def main():
    parser = argparse.ArgumentParser(description="Bronze -> Silver")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD, ngày theo giờ New York")
    parser.add_argument("--end-date", help="YYYY-MM-DD, mặc định = start-date")
    parser.add_argument("--steps", default=",".join(STEPS), help="weather,link_info,traffic")
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date).isoformat()
    end = date.fromisoformat(args.end_date or args.start_date).isoformat()
    if end < start:
        parser.error("--end-date phải >= --start-date")
    steps = [s.strip() for s in args.steps.split(",")]
    unknown = set(steps) - set(STEPS)
    if unknown:
        parser.error(f"bước không hợp lệ: {unknown}")

    spark = build_spark("bronze_to_silver")
    job_run_id = str(uuid.uuid4())
    log.info("job_run_id = %s, các bước: %s, khoảng ngày: %s -> %s", job_run_id, steps, start, end)

    # Thứ tự cố định: link_info cần weather_cleaned để lấy tâm ô
    if "weather" in steps:
        run_weather(spark, job_run_id)
    if "link_info" in steps:
        run_link_info(spark)
    if "traffic" in steps:
        run_traffic(spark, start, end, job_run_id)

    spark.stop()

if __name__ == "__main__":
    main()