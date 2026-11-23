from common.config import (SILVER_NAMESPACE, SILVER_TRAFFIC_CLEANED, SILVER_WEATHER_CLEANED,
                           SILVER_LINK_INFO, SILVER_INGEST_AUDIT, SILVER_TRAFFIC_QUARANTINE)
from common.schemas import (SILVER_TRAFFIC_FIELDS, SILVER_WEATHER_FIELDS, SILVER_LINK_INFO_FIELDS,
                            SILVER_INGEST_AUDIT_FIELDS, SILVER_TRAFFIC_QUARANTINE_FIELDS, to_ddl)
from common.spark_session import build_spark
from common.logger import get_logger
from pyspark.sql import SparkSession

log = get_logger("ddl_silver")

# Bảng Iceberg đã tạo không tự đổi schema khi sửa dict trong schemas.py.
# Hàm này chỉ THÊM các cột có trong dict mà bảng chưa có, đặt đúng vị trí theo thứ tự trong dict.
# Không đổi kiểu, không xóa cột. Dòng cũ có giá trị null ở cột mới.
def add_missing_columns(spark: SparkSession, table: str, fields: dict):
    existing = set(spark.table(table).columns)
    prev = None
    for col, typ in fields.items():
        if col not in existing:
            position = f"AFTER `{prev}`" if prev else "FIRST"
            spark.sql(f"ALTER TABLE {table} ADD COLUMN `{col}` {typ} {position}")
            log.info("[%s] đã thêm cột %s %s %s", table, col, typ, position)
        prev = col

# Hàm tạo các table trong namespace
def create_table(spark:SparkSession, table: str, fields: dict, partition_by: str | None = None):
    columns = to_ddl(fields)
    partition = f"PARTITIONED BY ({partition_by})" if partition_by else ""

    query = f"""
        CREATE TABLE IF NOT EXISTS {table} ({columns})
        USING ICEBERG
        {partition}
        TBLPROPERTIES (
            'format-version' = '2',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '50'
        )
    """
    spark.sql(query)
    add_missing_columns(spark, table, fields)     # bảng đã có từ trước: bổ sung cột mới (vd event_hour)

def main():
    spark = build_spark("ddl_silver")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")

    # traffic: chia partition theo tháng của cột date (ngày New York)
    # date: 2025-01-01 13:00 sẽ được transform về month = 2025-01 và được partition theo month
    create_table(spark, SILVER_TRAFFIC_CLEANED, SILVER_TRAFFIC_FIELDS, partition_by="months(`date`)")
    # 3 bảng nhỏ: không partition
    create_table(spark, SILVER_WEATHER_CLEANED, SILVER_WEATHER_FIELDS)
    create_table(spark, SILVER_LINK_INFO, SILVER_LINK_INFO_FIELDS)
    create_table(spark, SILVER_INGEST_AUDIT, SILVER_INGEST_AUDIT_FIELDS)
    # bổ sung tạo bảng traffic quarantine
    create_table(spark, SILVER_TRAFFIC_QUARANTINE, SILVER_TRAFFIC_QUARANTINE_FIELDS)
    # Kiểm tra
    spark.sql(f"SHOW TABLES IN {SILVER_NAMESPACE}").show()
    spark.sql(f"DESCRIBE TABLE {SILVER_TRAFFIC_CLEANED}").show(50, truncate=False)
    spark.sql(f"DESCRIBE TABLE {SILVER_INGEST_AUDIT}").show(50, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()