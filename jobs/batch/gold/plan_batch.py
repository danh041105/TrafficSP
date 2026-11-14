"""Lập kế hoạch một lượt Batch cho Airflow: xử lý khoảng nào, dựa trên DỮ LIỆU chứ không dựa trên ngày logic của Airflow
(dữ liệu phát lại là năm 2025 trong khi Airflow chạy năm 2026).

  silver_start/silver_end : ngày New York cần (re)build Silver = từ ngày của T_batch hiện hành (lùi 1 ngày vì lệch múi giờ)
                            tới ngày lớn nhất có trong Bronze.
  utc_start/utc_end       : ngày UTC cần recompute Batch View = từ ngày của T_batch tới ngày của event mới nhất.
  first_run               : chưa từng PUBLISHED -> lấy từ ngày nhỏ nhất có trong Bronze.
In ra 1 dòng JSON cuối cùng (Airflow BashOperator đẩy dòng cuối vào XCom).
Chạy: spark-submit .../plan_batch.py [--from-day 2025-12-01]
"""
import argparse
import datetime as dt
import json

from pyspark.sql import functions as F

from common import config
from common import lambda_control as lc
from common.spark_session import build_spark
from common.time_rules import NY


def ny_to_utc_day(s: str) -> dt.date:
    local = dt.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=NY)
    return local.astimezone(dt.timezone.utc).date()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-day", help="ép ngày UTC bắt đầu (backfill)")
    args = ap.parse_args()
    spark = build_spark("plan_batch")
    t_old = lc.current_t_batch([r.asDict() for r in spark.table(config.GOLD_LAMBDA_CONTROL).collect()])
    b = spark.table(config.BRONZE_TRAFFIC_TABLE).agg(F.min("data_as_of").alias("mn"), F.max("data_as_of").alias("mx")).collect()[0]
    if b["mx"] is None:
        print(json.dumps({"skip": True, "reason": "Bronze rỗng"})); spark.stop(); return
    if args.from_day:
        utc_start = dt.date.fromisoformat(args.from_day)
    elif t_old == lc.EPOCH:
        utc_start = ny_to_utc_day(b["mn"])
    else:
        utc_start = t_old.date()
    utc_end = ny_to_utc_day(b["mx"])
    plan = {"skip": utc_end < utc_start, "t_batch": str(t_old), "first_run": t_old == lc.EPOCH,
            "silver_start": str(utc_start - dt.timedelta(days=1)), "silver_end": b["mx"][:10],
            "utc_start": str(utc_start), "utc_end": str(utc_end)}
    print(json.dumps(plan))
    spark.stop()


if __name__ == "__main__":
    main()
