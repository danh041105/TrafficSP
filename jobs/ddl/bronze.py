# Tạo sẵn các bảng + namespace trong bronze
# bronze.py
from common.config import BRONZE_NAMESPACE, BRONZE_TRAFFIC_TABLE, BRONZE_WEATHER_TABLE
from common.schemas import TRAFFIC_FIELDS, WEATHER_FIELDS, METADATA_FIELDS, to_ddl
from common.spark_session import build_spark
from pyspark.sql import SparkSession

# Hàm tạo các table trong namespace
def create_table(spark: SparkSession, table: str, fields: dict, partition_by: str | None = None):
    # kết hợp 2 dict với nhau
    all_fields = {**fields, **METADATA_FIELDS}
    columns = to_ddl(all_fields)
    partition = f"PARTITIONED BY ({partition_by})" if partition_by else ""

    # tạo bảng
    query = f"""
        CREATE TABLE IF NOT EXISTS {table} ({columns})
        USING iceberg
        {partition}
        TBLPROPERTIES (
            'format-version' = '2',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '50'
        )
    """
    spark.sql(query)

# Tạo các namespace
def main():
    spark = build_spark("ddl_bronze")
    # tạo namespace có tên là bronze
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BRONZE_NAMESPACE}")

    # tạo 2 bảng trong namespace
    create_table(spark, BRONZE_TRAFFIC_TABLE, TRAFFIC_FIELDS, partition_by="event_month")
    create_table(spark, BRONZE_WEATHER_TABLE, WEATHER_FIELDS)

    # Kiểm tra các namespace đã được tạo hay chưa
    spark.sql(f"SHOW TABLES IN {BRONZE_NAMESPACE}").show()
    spark.sql(f"DESCRIBE TABLE {BRONZE_TRAFFIC_TABLE}").show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()