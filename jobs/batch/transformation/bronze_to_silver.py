# Bronze -> Silver: weather_cleaned, link_info, traffic_cleaned và ingest_audit.
# Ngày truyền vào là ngày theo giờ New York
# Lệnh chạy:
#  spark-submit .../bronze_to_silver.py --start-date 2025-01-01 --end-date 2025-09-30
#  spark-submit .../bronze_to_silver.py --start-date 2025-10-05 --steps weather,traffic
# Chạy lại một vài giờ traffic: xem rerun_silver_hours.py
import argparse
import uuid
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, functions as F
from common.config import (BRONZE_TRAFFIC_TABLE, BRONZE_WEATHER_TABLE, SILVER_TRAFFIC_CLEANED, SILVER_WEATHER_CLEANED,
                           SILVER_LINK_INFO, SILVER_INGEST_AUDIT, SILVER_TRAFFIC_QUARANTINE, TRAFFIC_TIMEZONE)
from common.schemas import (SILVER_TRAFFIC_FIELDS, SILVER_WEATHER_FIELDS,
                            SILVER_LINK_INFO_FIELDS, SILVER_INGEST_AUDIT_FIELDS, SILVER_TRAFFIC_QUARANTINE_FIELDS, conform)
from common.spark_session import build_spark
from common.logger import get_logger
from common.data_cleaning import (cast_traffic, enriched_traffic_time, drop_null_keys, drop_invalid_status,
                                  drop_dst_duplicated_hours, deduplicate, dst_reason, parse_weather_time)
from batch.transformation.link_location import (clean_link_points, add_median_point,
                                                assign_nearest_cell)
STEPS = ["weather", "link_info", "traffic"]
CACHE = StorageLevel.MEMORY_AND_DISK
NY = ZoneInfo(TRAFFIC_TIMEZONE)

log = get_logger("bronze_to_silver")


def date_range_sql(start: date, end: date) -> str:
    # Điều kiện chọn các ngày NY từ start đến end (tính cả hai đầu)
    return f"(`date` BETWEEN DATE '{start.isoformat()}' AND DATE '{end.isoformat()}')"


def ny_to_utc(local: datetime) -> datetime:
    # Giờ NY (không múi giờ) -> giờ UTC (không múi giờ). Giờ DST lặp lấy lần đầu (fold=0)
    return local.replace(tzinfo=NY).astimezone(timezone.utc).replace(tzinfo=None)


def write_audit(source: str, event_date: Column, event_hour: Column, raw: DataFrame, before_dedup: DataFrame,
                after_dedup: DataFrame, job_run_id: str) -> None:
    # 1 dòng audit = 1 lần chạy × nguồn × (event_date, event_hour) × kafka_partition
    keys = ["event_date", "event_hour", "kafka_partition"]

    def by_key(df):
        return (df.withColumn("event_date", event_date)
                  .withColumn("event_hour", event_hour)
                  .groupBy(*keys))
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


def run_weather(spark, start: date, end: date, job_run_id: str) -> None:
    raw = parse_weather_time(spark.table(BRONZE_WEATHER_TABLE)).persist(CACHE)
    cleaned = drop_null_keys(raw, ["cell_id", "weather_utc"])
    silver = deduplicate(cleaned, ["cell_id", "weather_utc"]).persist(CACHE)
    out = conform(silver.withColumn("processed_ts", F.current_timestamp()), SILVER_WEATHER_FIELDS)
    out.writeTo(SILVER_WEATHER_CLEANED).overwrite(F.lit(True))          # bảng nhỏ: ghi đè toàn bảng

    # Bảng ghi đè toàn bộ, nhưng audit chỉ ghi cho các giờ UTC ứng với khoảng ngày NY của lần chạy này.
    # Nếu không, mỗi lần chạy (vd DAG mỗi giờ) sẽ ghi lại audit cho toàn bộ weather đã có.
    lo = ny_to_utc(datetime.combine(start, time(0)))
    hi = ny_to_utc(datetime.combine(end + timedelta(days=1), time(0)))
    in_run = (F.col("weather_utc") >= F.lit(lo)) & (F.col("weather_utc") < F.lit(hi))
    write_audit("weather", F.to_date("weather_utc"), F.hour("weather_utc"),
                raw.filter(in_run), cleaned.filter(in_run), silver.filter(in_run), job_run_id)

    log.info("[weather] đã ghi %d dòng", silver.count())
    raw.unpersist()
    silver.unpersist()


def run_link_info(spark) -> None:
    links = cast_traffic(spark.table(BRONZE_TRAFFIC_TABLE))
    # Snapshot mô tả road lấy theo event time; replay cũ đến sau không được
    # ghi đè thuộc tính của một sự kiện mới hơn.
    latest_event = links.groupBy("link_id").agg(F.max("data_as_of").alias("data_as_of"))
    links = links.join(latest_event, ["link_id", "data_as_of"], "inner")
    links = deduplicate(links, ["link_id"])                      # bản mới nhất của mỗi link
    links = clean_link_points(links)
    links = drop_null_keys(links, ["link_id", "link_points"])
    links = add_median_point(links)

    cells = spark.table(SILVER_WEATHER_CLEANED).select("cell_id", "latitude", "longitude").distinct()
    out = assign_nearest_cell(links, cells).withColumn("processed_ts", F.current_timestamp())
    conform(out, SILVER_LINK_INFO_FIELDS).writeTo(SILVER_LINK_INFO).overwrite(F.lit(True))

    log.info("[link_info] đã ghi %d link", spark.table(SILVER_LINK_INFO).count())


def traffic_in_scope(bronze: DataFrame, start: date, end: date) -> DataFrame:
    """Giữ cả lỗi timestamp để quarantine thay vì bỏ qua do date=NULL.

    Caller đã lọc event_month. Khi cả timestamp và date prefix đều hỏng, chỉ
    biết tháng nguồn: quarantine có thể bao gồm lỗi không định ngày trong
    các tháng chạm vào bởi một lần chạy phạm vi ngày nhỏ hơn tháng.
    """
    scoped = bronze.withColumn("_source_date", F.to_date(F.substring("data_as_of", 1, 10)))
    raw = enriched_traffic_time(cast_traffic(scoped))
    in_dates = F.col("date").between(F.lit(start).cast("date"), F.lit(end).cast("date"))
    malformed_in_scope = F.col("date").isNull() & (
        F.col("_source_date").isNull()
        | F.col("_source_date").between(F.lit(start).cast("date"), F.lit(end).cast("date"))
    )
    return raw.filter(in_dates | malformed_in_scope)


def traffic_quarantine(raw: DataFrame, years: tuple[int, ...]) -> DataFrame:
    missing_key = F.when(F.col("link_id").isNull() | F.col("traffic_utc").isNull(), F.lit("missing_key"))
    quarantine = (raw.withColumn("reason", F.coalesce(dst_reason(years), missing_key))
                     .filter(F.col("reason").isNotNull())
                     .withColumn("date", F.coalesce(F.col("date"), F.col("_source_date"))))
    # Một lỗi nguồn chỉ ghi một lần dù Kafka record có mặt nhiều lần ở Bronze.
    return deduplicate(quarantine, ["kafka_partition", "kafka_offset"])


def run_traffic(spark, start: date, end: date, cond: str, job_run_id: str) -> None:
    # start/end: ngày NY đầu/cuối của khoảng (lọc partition Bronze, tính giờ DST).
    # cond: điều kiện SQL trên date/hour chọn đúng phần cần tính lại; dùng cho cả lọc lẫn DELETE
    # -> phần bị xóa luôn đúng bằng phần được ghi lại. Theo ngày: date_range_sql; theo giờ: rerun_silver_hours.py.
    # data_as_of là giờ New York nên event_month cũng theo giờ New York: lọc đúng partition Bronze
    bronze = (spark.table(BRONZE_TRAFFIC_TABLE)
              .filter(F.col("event_month").between(start.strftime("%Y-%m"), end.strftime("%Y-%m"))))
    # Dòng có ngày: chỉ giữ phần thỏa cond (theo ngày hoặc theo giờ) để khớp với DELETE bên dưới.
    # Dòng data_as_of hỏng (date null): giữ lại cho quarantine; quarantine ghi bằng MERGE theo khóa Kafka
    # nên chạy lại nhiều lần (kể cả theo giờ) cũng không bị trùng.
    raw = (traffic_in_scope(bronze, start, end)
           .filter(F.expr(cond) | F.col("date").isNull())
           .persist(CACHE))
    # Các năm của khoảng đang xử lý, vd (2025,) hoặc (2025, 2026) nếu vắt qua năm mới: để tính giờ DST
    years = tuple(range(start.year, end.year + 1))
    cleaned = (
        raw
        .transform(drop_null_keys, ["link_id", "traffic_utc"])
        .transform(drop_dst_duplicated_hours, years)
        .transform(drop_invalid_status)
    )
    silver = cleaned.transform(deduplicate, ["link_id", "traffic_utc"]).persist(CACHE)

    # Quarantine: dòng bị loại vì giờ DST (lặp/không tồn tại) hoặc thiếu link_id.
    quarantine = traffic_quarantine(raw, years).persist(CACHE)

    # Xóa đúng phần đang xử lý rồi ghi lại -> chạy lại bao nhiêu lần cũng không bị trùng
    spark.sql(f"DELETE FROM {SILVER_TRAFFIC_QUARANTINE} WHERE {cond}")
    # MERGE khóa Kafka làm rerun idempotent cả với lỗi không thể suy date.
    view = f"traffic_quarantine_{uuid.uuid4().hex}"
    conform(quarantine.withColumn("processed_ts", F.current_timestamp()),
            SILVER_TRAFFIC_QUARANTINE_FIELDS).createOrReplaceTempView(view)
    try:
        spark.sql(f"""
            MERGE INTO {SILVER_TRAFFIC_QUARANTINE} t USING {view} s
            ON t.kafka_partition <=> s.kafka_partition AND t.kafka_offset <=> s.kafka_offset
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(view)
    for r in quarantine.groupBy("reason").count().collect():
        log.info("[traffic] quarantine %s: %d dòng", r["reason"], r["count"])

    spark.sql(f"DELETE FROM {SILVER_TRAFFIC_CLEANED} WHERE {cond}")
    out = conform(silver.withColumn("processed_ts", F.current_timestamp()), SILVER_TRAFFIC_FIELDS)
    out.writeTo(SILVER_TRAFFIC_CLEANED).append()
    write_audit("traffic", F.col("date"), F.col("hour"), raw, cleaned, silver, job_run_id)

    log.info("[traffic] %s: đã ghi %d dòng", cond, silver.count())
    raw.unpersist()
    silver.unpersist()
    quarantine.unpersist()

def main():
    parser = argparse.ArgumentParser(description="Bronze -> Silver")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD, ngày theo giờ New York")
    parser.add_argument("--end-date", help="YYYY-MM-DD, mặc định = start-date")
    parser.add_argument("--steps", default=",".join(STEPS), help="weather,link_info,traffic")
    args = parser.parse_args()

    try:
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date or args.start_date)
    except ValueError as e:
        parser.error(f"sai định dạng ngày: {e}")
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
        run_weather(spark, start, end, job_run_id)
    if "link_info" in steps:
        run_link_info(spark)
    if "traffic" in steps:
        run_traffic(spark, start, end, date_range_sql(start, end), job_run_id)

    spark.stop()

if __name__ == "__main__":
    main()

