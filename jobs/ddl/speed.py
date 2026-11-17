# Tạo namespace + bảng Speed trên Iceberg (chỉ Speed Layer ghi). Idempotent.
#   traffic_5min_rt : append-only (Iceberg streaming sink), phân vùng ngày UTC. Mỗi micro-batch = 1 commit
#                     -> nhiều file nhỏ: cần compaction định kỳ (rewrite_data_files) - xem docs/streaming_runbook.md.
#   traffic_latest  : ~124 dòng, MERGE INTO mỗi micro-batch; copy-on-write (bảng nhỏ: ghi lại 1 file rẻ hơn delete file).
#   weather_obs     : append-only, phân vùng ngày UTC.
# Chạy: docker exec traffic-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/ddl/speed.py
from common import config
from common.schemas import SPEED_TRAFFIC_5MIN_FIELDS, SPEED_TRAFFIC_LATEST_FIELDS, SPEED_WEATHER_OBS_FIELDS
from common.spark_session import build_spark
from ddl.gold import create_table

STREAM_PROPS = {
    "commit.retry.num-retries": "10",              # retry khi xung đột commit (vd chạy song song compaction)
    "write.distribution-mode": "none",             # micro-batch nhỏ: không shuffle thêm trước khi ghi
    "commit.manifest.min-count-to-merge": "50",    # gộp manifest khi nhiều commit nhỏ
}


def main():
    spark = build_spark("ddl_speed")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.SPEED_NAMESPACE}")
    create_table(spark, config.SPEED_TRAFFIC_5MIN, SPEED_TRAFFIC_5MIN_FIELDS, "days(window_start_utc)", STREAM_PROPS)
    create_table(spark, config.SPEED_TRAFFIC_LATEST, SPEED_TRAFFIC_LATEST_FIELDS, None,
                 {**STREAM_PROPS, "write.merge.mode": "copy-on-write", "write.update.mode": "copy-on-write"})
    create_table(spark, config.SPEED_WEATHER_OBS, SPEED_WEATHER_OBS_FIELDS, "days(time_utc)", STREAM_PROPS)
    spark.sql(f"SHOW TABLES IN {config.SPEED_NAMESPACE}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
