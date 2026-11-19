# Tạo sẵn namespace + các bảng của speed layer (lakehouse.speed)
# Lệnh chạy: spark-submit /opt/airflow/jobs/ddl/speed.py
from common.config import (SPEED_NAMESPACE, SPEED_TRAFFIC_5MIN, SPEED_TRAFFIC_LATEST,
                           SPEED_WEATHER_OBS)
from common.schemas import (SPEED_TRAFFIC_5MIN_FIELDS, SPEED_TRAFFIC_LATEST_FIELDS,
                            SPEED_WEATHER_OBS_FIELDS, to_ddl)
from common.spark_session import build_spark
from pyspark.sql import SparkSession    

# Hàm tạo các table trong namespace speed
# extra_fields: các cột ghép thêm vào sau fields (giống METADATA_FIELDS ở Bronze),
# dùng để ghép AGG_TRAFFIC_MEASURES vào bảng traffic_5min.
# Bảng speed không partition: dữ liệu nhỏ (chỉ giữ phần batch chưa xử lý, được dọn hằng ngày)
def create_table(spark: SparkSession, table: str, fields: dict):
    # kết hợp 2 dict với nhau
    columns = to_ddl(fields)
    query = f"""
        CREATE TABLE IF NOT EXISTS {table} ({columns})
        USING iceberg
        TBLPROPERTIES (
            -- Thuộc tính riêng cho bảng ghi liên tục từ streaming
            'commit.retry.num-retries' = '10',            -- thử commit lại khi xung đột (vd chạy song song với compaction/DELETE)
            'write.distribution-mode' = 'none',           -- micro-batch nhỏ: không shuffle thêm trước khi ghi
            'commit.manifest.min-count-to-merge' = '50',  -- gộp manifest khi tích lũy nhiều commit nhỏ

            -- Thuộc tính chung giống bảng Bronze/Silver
            'format-version' = '2',                       -- Iceberg v2: hỗ trợ MERGE/DELETE theo dòng
            'write.metadata.delete-after-commit.enabled' = 'true',  -- tự xóa metadata cũ sau mỗi commit
            'write.metadata.previous-versions-max' = '50'           -- chỉ giữ 50 phiên bản metadata gần nhất
        )
    """
    spark.sql(query)

def main():
    spark = build_spark("ddl_speed")
    # Tạo namespace
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SPEED_NAMESPACE}")
    create_table(spark, SPEED_TRAFFIC_5MIN, SPEED_TRAFFIC_5MIN_FIELDS)
    create_table(spark, SPEED_TRAFFIC_LATEST, SPEED_TRAFFIC_LATEST_FIELDS)
    create_table(spark, SPEED_WEATHER_OBS, SPEED_WEATHER_OBS_FIELDS)
    
    # Kiểm tra
    spark.sql(f"SHOW TABLES IN {SPEED_NAMESPACE}").show()
    spark.sql(f"DESCRIBE TABLE {SPEED_TRAFFIC_5MIN}").show(50, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
