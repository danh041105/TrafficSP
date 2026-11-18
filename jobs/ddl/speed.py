# Tạo sẵn namespace + các bảng của speed layer (lakehouse.speed)
# Lệnh chạy: spark-submit /opt/airflow/jobs/ddl/speed.py
from common.config import (SPEED_NAMESPACE, SPEED_TRAFFIC_5MIN, SPEED_TRAFFIC_LATEST,
                           SPEED_WEATHER_OBS)
from common.schemas import (SPEED_TRAFFIC_5MIN_FIELDS, SPEED_TRAFFIC_LATEST_FIELDS,
                            SPEED_WEATHER_OBS_FIELDS, AGG_TRAFFIC_MEASURES, to_ddl)
from common.spark_session import build_spark
from pyspark.sql import SparkSession

# Thuộc tính riêng cho bảng ghi liên tục từ streaming (nhiều commit nhỏ, 30 s/lần)
STREAM_PROPS = {
    "commit.retry.num-retries": "10",              # retry khi xung đột commit (vd chạy song song compaction)
    "write.distribution-mode": "none",             # micro-batch nhỏ: không shuffle thêm trước khi ghi
    "commit.manifest.min-count-to-merge": "50",    # gộp manifest khi nhiều commit nhỏ
}

# Thuộc tính chung giống bảng Bronze/Silver
BASE_PROPS = {
    "format-version": "2",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
}

# Hàm tạo các table trong namespace speed
# extra_fields: các cột ghép thêm vào sau fields (giống METADATA_FIELDS ở Bronze),
# dùng để ghép AGG_TRAFFIC_MEASURES vào bảng traffic_5min.
def create_table(spark: SparkSession, table: str, fields: dict, partition_by: str | None = None, extra_fields: dict | None = None):
    # kết hợp 2 dict với nhau
    all_fields = {**fields, **(extra_fields or {})}
    columns = to_ddl(all_fields)
    partition = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    query = f"""
        CREATE TABLE IF NOT EXISTS {table} ({columns})
        USING iceberg
        {partition}
        TBLPROPERTIES (
            'commit.retry.num-retries' = '10',
            'write.distribution-mode' = 'none',                 
            'commit.manifest.min-count-to-merge' = '50',
            'format-version' = '2',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '50'
        )
    """
    spark.sql(query)

def main():
    spark = build_spark("ddl_speed")
    # Tạo namespace
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPEED_NAMESPACE}")
    # traffic_5min: ~124 link x 288 cửa sổ/ngày -> partition theo ngày (UTC) của cửa sổ
    # để DELETE dọn dữ liệu đã có trong Gold chỉ chạm vài partition
    create_table(spark, SPEED_TRAFFIC_5MIN, SPEED_TRAFFIC_5MIN_FIELDS,
                 partition_by="days(window_start_utc)", extra_fields=AGG_TRAFFIC_MEASURES)
    # 2 bảng nhỏ: không partition
    create_table(spark, SPEED_TRAFFIC_LATEST, SPEED_TRAFFIC_LATEST_FIELDS)
    create_table(spark, SPEED_WEATHER_OBS, SPEED_WEATHER_OBS_FIELDS)

    # Kiểm tra
    spark.sql(f"SHOW TABLES IN {SPEED_NAMESPACE}").show()
    spark.sql(f"DESCRIBE TABLE {SPEED_TRAFFIC_5MIN}").show(50, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
