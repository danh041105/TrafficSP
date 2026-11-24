# Tạo namespace + bảng Speed trên Iceberg (chỉ Speed Layer ghi). Idempotent.
#   traffic_5min_rt : append-only (Iceberg streaming sink), phân vùng ngày UTC. Mỗi micro-batch = 1 commit
#                     -> nhiều file nhỏ: cần compaction định kỳ (rewrite_data_files) - xem docs/streaming_runbook.md.
#   traffic_latest  : ~124 dòng, MERGE INTO mỗi micro-batch; copy-on-write (bảng nhỏ: ghi lại 1 file rẻ hơn delete file).
#   weather_obs     : append-only, phân vùng ngày UTC.
# Chạy: docker exec traffic-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/ddl/speed.py
from common import config
from common.schemas import SPEED_TRAFFIC_5MIN_FIELDS, SPEED_TRAFFIC_LATEST_FIELDS, SPEED_WEATHER_OBS_FIELDS
from common.spark_session import build_spark
from ddl.gold import DERIVED_5MIN, create_table

STREAM_PROPS = {
    "commit.retry.num-retries": "10",              # retry khi xung đột commit (vd chạy song song compaction)
    "write.distribution-mode": "none",             # micro-batch nhỏ: không shuffle thêm trước khi ghi
    "commit.manifest.min-count-to-merge": "50",    # gộp manifest khi nhiều commit nhỏ
}


# Đổi tên từ bản gốc (valid_*/obs_*) hoặc bản rút gọn (event_time, speed, ...) sang tên theo quy ước gold-dev
LATEST_RENAMES = {"valid_event_time": "reading_ts_utc", "event_time": "reading_ts_utc",
                  "speed": "speed_mph", "travel_time": "travel_time_sec", "valid_kafka_offset": "kafka_offset",
                  "obs_event_time": "last_seen_ts_utc", "last_seen_time": "last_seen_ts_utc",
                  "obs_is_valid": "last_seen_valid", "obs_invalid_reason": "invalid_reason"}
FIVEMIN_RENAMES = {"sum_speed": "sum_speed_mph", "sum_travel_time": "sum_travel_time_sec"}
LATEST_DROPS = ("valid_data_as_of", "valid_kafka_partition", "valid_kafka_ts", "obs_data_as_of", "obs_speed",
                "obs_travel_time", "obs_kafka_partition", "obs_kafka_offset", "obs_kafka_ts", "run_id", "batch_id")


def main():
    spark = build_spark("ddl_speed")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.SPEED_NAMESPACE}")
    # Bảng đã có từ bản trước: bỏ cột suy ra được / không dùng, đổi tên cột latest (xem common/schemas.py)
    create_table(spark, config.SPEED_TRAFFIC_5MIN, SPEED_TRAFFIC_5MIN_FIELDS, "days(window_start_utc)", STREAM_PROPS,
                 renames=FIVEMIN_RENAMES, drops=DERIVED_5MIN + ("kafka_ts_max",))
    # Dòng có từ trước khi thêm khoá lịch: điền date_key / hour_key (giờ New York) - idempotent, chỉ dòng còn NULL.
    # Chạy khi Speed Layer đang dừng (UPDATE copy-on-write có thể xung đột với commit append của stream).
    tz = config.CALENDAR_TIMEZONE
    spark.sql(f"UPDATE {config.SPEED_TRAFFIC_5MIN} SET "
              f"date_key = CAST(date_format(from_utc_timestamp(window_start_utc, '{tz}'), 'yyyyMMdd') AS INT), "
              f"hour_key = hour(from_utc_timestamp(window_start_utc, '{tz}')) WHERE date_key IS NULL")
    create_table(spark, config.SPEED_TRAFFIC_LATEST, SPEED_TRAFFIC_LATEST_FIELDS, None,
                 {**STREAM_PROPS, "write.merge.mode": "copy-on-write", "write.update.mode": "copy-on-write"},
                 renames=LATEST_RENAMES, drops=LATEST_DROPS)
    create_table(spark, config.SPEED_WEATHER_OBS, SPEED_WEATHER_OBS_FIELDS, "days(time_utc)", STREAM_PROPS,
                 drops=("run_id", "kafka_partition", "kafka_ts", "processed_at"))
    spark.sql(f"SHOW TABLES IN {config.SPEED_NAMESPACE}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
