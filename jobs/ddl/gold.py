# Tạo namespace + bảng Gold (Batch View) trên Iceberg. Idempotent (CREATE ... IF NOT EXISTS).
# Chạy: docker exec -e PYTHONPATH=/opt/spark/jobs traffic-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/ddl/gold.py
from pyspark.sql import SparkSession

from common.config import GOLD_NAMESPACE, GOLD_TRAFFIC_5MIN, GOLD_TRAFFIC_HOURLY, GOLD_LINK_BASELINE
from common.schemas import (GOLD_TRAFFIC_5MIN_FIELDS, GOLD_TRAFFIC_HOURLY_FIELDS, GOLD_LINK_BASELINE_FIELDS, to_ddl)
from common.spark_session import build_spark


def create_table(spark: SparkSession, table: str, fields: dict, partition_by: str | None = None):
    partition = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} ({to_ddl(fields)})
        USING iceberg
        {partition}
        TBLPROPERTIES (
            'format-version' = '2',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '50'
        )
    """)


def main():
    spark = build_spark("ddl_gold")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
    # Phân vùng theo NGÀY UTC: Batch View ghi lại từng ngày (overwritePartitions) -> chạy lại không sinh trùng
    create_table(spark, GOLD_TRAFFIC_5MIN, GOLD_TRAFFIC_5MIN_FIELDS, partition_by="days(window_start)")
    create_table(spark, GOLD_TRAFFIC_HOURLY, GOLD_TRAFFIC_HOURLY_FIELDS, partition_by="days(hour_utc)")
    create_table(spark, GOLD_LINK_BASELINE, GOLD_LINK_BASELINE_FIELDS)
    spark.sql(f"SHOW TABLES IN {GOLD_NAMESPACE}").show()
    spark.stop()


if __name__ == "__main__":
    main()
