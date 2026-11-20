# Chạy lại Silver traffic (traffic_cleaned, traffic_quarantine, ingest_audit) cho một khoảng giờ New York.
# Chỉ dùng khi cần sửa vài giờ (vd Bronze nhận thêm dữ liệu muộn), không nằm trong luồng chạy thường ngày.
# Dùng chung run_traffic với bronze_to_silver.py: cùng các bước làm sạch, chỉ khác điều kiện chọn giờ.
# Lệnh chạy:
#  spark-submit .../rerun_silver_hours.py --start-hour 2025-10-05T13
#  spark-submit .../rerun_silver_hours.py --start-hour 2025-10-05T22 --end-hour 2025-10-06T01
import argparse
import uuid
from datetime import datetime
from common.spark_session import build_spark
from common.logger import get_logger
from batch.transformation.bronze_to_silver import run_traffic

HOUR_FMT = "%Y-%m-%dT%H"   # 2025-10-05T13 = giờ 13 ngày 05/10 theo giờ New York

log = get_logger("rerun_silver_hours")


def hour_range_sql(start: datetime, end: datetime) -> str:
    # Điều kiện chọn các giờ NY từ start đến end (tính cả hai đầu) trên cột date + hour.
    # Chỉ dùng so sánh trên cột trần để Iceberg đẩy xuống được: bỏ partition months(date), bỏ file theo min/max.
    d0, d1 = start.date().isoformat(), end.date().isoformat()
    if d0 == d1:
        return f"(`date` = DATE '{d0}' AND `hour` BETWEEN {start.hour} AND {end.hour})"
    return (f"((`date` = DATE '{d0}' AND `hour` >= {start.hour})"
            f" OR (`date` > DATE '{d0}' AND `date` < DATE '{d1}')"
            f" OR (`date` = DATE '{d1}' AND `hour` <= {end.hour}))")


def main():
    parser = argparse.ArgumentParser(description="Chạy lại Silver traffic theo giờ New York")
    parser.add_argument("--start-hour", required=True, help="YYYY-MM-DDTHH, giờ NY, vd 2025-10-05T13")
    parser.add_argument("--end-hour", help="YYYY-MM-DDTHH, mặc định = start-hour (tính cả hai đầu)")
    args = parser.parse_args()

    try:
        start = datetime.strptime(args.start_hour, HOUR_FMT)
        end = datetime.strptime(args.end_hour or args.start_hour, HOUR_FMT)
    except ValueError as e:
        parser.error(f"sai định dạng giờ (cần YYYY-MM-DDTHH): {e}")
    if end < start:
        parser.error("--end-hour phải >= --start-hour")

    spark = build_spark("rerun_silver_hours")
    job_run_id = str(uuid.uuid4())
    log.info("job_run_id = %s, khoảng giờ NY: %s -> %s",
             job_run_id, start.strftime(HOUR_FMT), end.strftime(HOUR_FMT))
    run_traffic(spark, start.date(), end.date(), hour_range_sql(start, end), job_run_id)
    spark.stop()


if __name__ == "__main__":
    main()
