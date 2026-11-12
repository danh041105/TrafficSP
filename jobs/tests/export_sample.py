# Xuất mẫu dữ liệu Silver ra file parquet (mỗi bảng 1 file) để xem/test trên máy.
# Chạy: docker exec -it traffic-airflow-scheduler spark-submit /opt/airflow/jobs/tests/export_sample.py --date 2025-01-01
# File nằm ở MinIO: lakehouse/exports/<bảng>_<ngày>/part-00000-....parquet
import argparse

from pyspark.sql import DataFrame, functions as F

from common.config import SILVER_TRAFFIC_CLEANED, SILVER_WEATHER_CLEANED, SILVER_LINK_INFO
from common.spark_session import build_spark

EXPORT_DIR = "s3a://lakehouse/exports"
NY_TZ = "America/New_York"
TABLES = ["traffic", "weather", "link_info"]


def export(df: DataFrame, name: str) -> None:
    """Gộp về 1 file rồi ghi đè thư mục cũ (chạy lại không sinh file thừa)."""
    path = f"{EXPORT_DIR}/{name}"
    df.coalesce(1).write.mode("overwrite").parquet(path)
    print(f"[{name}] đã xuất {df.count()} dòng ra {path}")


def main():
    parser = argparse.ArgumentParser(description="Xuất mẫu dữ liệu Silver")
    parser.add_argument("--date", default="2025-01-01", help="YYYY-MM-DD, ngày theo giờ New York")
    parser.add_argument("--tables", default=",".join(TABLES), help="traffic,weather,link_info")
    args = parser.parse_args()

    tables = [t.strip() for t in args.tables.split(",")]
    unknown = set(tables) - set(TABLES)
    if unknown:
        parser.error(f"bảng không hợp lệ: {unknown}")

    spark = build_spark("export_sample")
    day = F.lit(args.date).cast("date")

    # Traffic: 1 ngày theo giờ New York (cột date), khoảng 124 link x 288 lần đo
    if "traffic" in tables:
        df = spark.table(SILVER_TRAFFIC_CLEANED).filter(F.col("date") == day)
        export(df, f"traffic_cleaned_{args.date}")

    # Weather: weather_utc là giờ UTC -> đổi sang giờ New York rồi lấy đúng ngày đó,
    # để khớp với file traffic cùng ngày (16 ô x 24 giờ = 384 dòng; ngày đổi giờ DST là 23/25 giờ)
    if "weather" in tables:
        ny_date = F.to_date(F.from_utc_timestamp("weather_utc", NY_TZ))
        df = spark.table(SILVER_WEATHER_CLEANED).filter(ny_date == day)
        export(df, f"weather_cleaned_{args.date}")

    # Link_info: bảng nhỏ (124 link), không phụ thuộc ngày -> xuất cả bảng
    if "link_info" in tables:
        export(spark.table(SILVER_LINK_INFO), "link_info")

    spark.stop()


if __name__ == "__main__":
    main()
