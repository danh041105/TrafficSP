"""Lập kế hoạch một lượt Batch cho Airflow: xử lý khoảng nào, dựa trên DỮ LIỆU chứ không dựa trên ngày logic của Airflow
(dữ liệu phát lại là năm 2025 trong khi Airflow chạy năm 2026).

  silver_start/silver_end : ngày New York cần (re)build Silver = từ ngày của batch_state hiện hành (lùi 1 ngày vì lệch múi giờ)
                            tới ngày lớn nhất có trong Bronze.
  utc_start/utc_end       : ngày UTC cần recompute Batch View = từ ngày của batch_state tới ngày của event mới nhất.
  first_run               : chưa từng PUBLISHED -> lấy từ ngày nhỏ nhất có trong Bronze.
  skip                    : batch_state KHÔNG THỂ tiến (mốc an toàn của dữ liệu mới nhất <= batch_state hiện hành)
                            -> không rebuild Silver/Gold vô ích. Mốc an toàn tính đúng như lambda_handoff
                            (floor5(event mới nhất - lateness)). Bản ghi đến muộn (event < batch_state, ingest sau lần
                            PUBLISHED cuối) được đếm vào late_rows để người vận hành chạy correction; không bị bỏ qua âm thầm.
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


def ny_to_utc(s: str) -> dt.datetime:
    local = dt.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=NY)
    return local.astimezone(dt.timezone.utc).replace(tzinfo=None)


def batch_can_advance(t_old: dt.datetime, safe_bound: dt.datetime, forced: bool = False) -> bool:
    """Lượt batch có ý nghĩa khi batch_state có thể tiến (mốc an toàn > batch_state), là lượt đầu, hoặc bị ép (--from-day)."""
    return forced or t_old == lc.EPOCH or safe_bound > t_old


def late_rows(spark, rows: list[dict], t_old: dt.datetime) -> int:
    """Bản ghi Bronze ingest SAU lần PUBLISHED gần nhất nhưng có giờ sự kiện TRƯỚC batch_state (dữ liệu đến muộn)."""
    pub = [r["created_at"] for r in rows if r["status"] == "PUBLISHED"]
    if not pub:
        return 0
    event_utc = F.to_utc_timestamp(F.to_timestamp("data_as_of"), "America/New_York")
    return spark.table(config.BRONZE_TRAFFIC_TABLE).where(F.col("ingest_ts") > F.lit(max(pub)))         .where(event_utc < F.lit(t_old)).count()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-day", help="ép ngày UTC bắt đầu (backfill) - luôn chạy, không xét skip")
    ap.add_argument("--lateness-minutes", type=int, default=int(config.SPEED_WATERMARK.split()[0]))
    args = ap.parse_args()
    spark = build_spark("plan_batch")
    rows = [r.asDict() for r in spark.table(config.GOLD_LAMBDA_CONTROL).collect()]
    t_old = lc.current_batch_state(rows)
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
    newest_utc = ny_to_utc(b["mx"])
    _, safe = lc.choose_t_new("auto", newest_utc, args.lateness_minutes)
    plan = {"skip": utc_end < utc_start, "batch_state": str(t_old), "first_run": t_old == lc.EPOCH,
            "silver_start": str(utc_start - dt.timedelta(days=1)), "silver_end": b["mx"][:10],
            "utc_start": str(utc_start), "utc_end": str(utc_end), "newest_event_utc": str(newest_utc), "safe_bound": str(safe)}
    if not batch_can_advance(t_old, safe, forced=bool(args.from_day)):
        plan["late_rows"] = late_rows(spark, rows, t_old)
        plan["skip"] = True
        plan["reason"] = (f"batch_state {t_old} không thể tiến: event mới nhất {newest_utc} UTC, mốc an toàn {safe}"
                          + (f"; {plan['late_rows']} bản ghi đến muộn trước batch_state -> cần lambda_handoff --correction"
                             if plan["late_rows"] else ""))
    print(json.dumps(plan))
    spark.stop()


if __name__ == "__main__":
    main()
