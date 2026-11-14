"""Lambda handoff hoàn toàn trên Iceberg: chuyển các cửa sổ 5 phút đã hoàn tất từ Speed sang Batch theo mốc T_batch.

  Unified (Trino):  batch  khi window_start_utc <  T_batch  (gold.traffic_5min_batch  FOR VERSION AS OF 'published')
                    speed  khi window_start_utc >= T_batch  (speed.traffic_5min_rt)
Iceberg KHÔNG có giao dịch nhiều bảng, nên publish là chuỗi bước mà mọi trạng thái trung gian đều nhất quán khi đọc:
  0. Khoá: pg_try_advisory_lock trên Postgres của catalog (handoff thứ hai thoát ngay). Phục hồi handoff dang dở.
  1. PREPARING  : ghi 1 dòng vào gold.lambda_control (append).
  2. Commit     : recompute Batch View các ngày UTC [ngày bắt đầu, ngày(T_new - 1s)] -> snapshot S5 / SH (nhánh main).
                  Trino CHƯA thấy (Trino đọc tag published).
  3. Kiểm tra   : trên CHÍNH snapshot S5/SH (VERSION AS OF) cho khoảng đã recompute [lo, T_new):
                  đủ so với Silver, không trùng grain, phủ mọi cửa sổ realtime, 1 giờ = tổng 5 phút. Lỗi -> FAILED, thoát 1.
  4. VALIDATED  : ghi dòng kèm S5/SH + kết quả kiểm tra.
  5. Dời tag    : ALTER TABLE ... REPLACE TAG published AS OF VERSION S5 (và SH). Lúc này Trino thấy dữ liệu batch mới cho
                  window < T_old (đã kiểm chứng) - T_batch vẫn cũ nên không chồng lấn, không hở.
  6. PUBLISHED  : ghi dòng t_batch = T_new. Từ đây unified lấy batch cho [T_old, T_new).
Crash ở bất kỳ bước nào: lần chạy sau (bước 0) đánh dấu FAILED, hoặc hoàn tất PUBLISHED nếu tag đã trỏ đúng snapshot đã
kiểm chứng. T_batch không bao giờ giảm. Dữ liệu muộn sau khi đã chốt: chạy lại với --correction --from-day <ngày>.

Chạy:
  spark-submit .../lambda_handoff.py --until auto
  spark-submit .../lambda_handoff.py --until 2025-12-02T08:00:00 --fail-at after_validate     # thử crash
  spark-submit .../lambda_handoff.py --until 2025-12-02T08:00:00 --no-recompute               # Airflow: batch đã ghi trước
"""
import argparse
import datetime as dt
import json
import sys
import uuid

from pyspark.sql import functions as F

from common import config
from common import lambda_control as lc
from common.logger import get_logger
from common.schemas import GOLD_LAMBDA_CONTROL_FIELDS
from common.spark_session import build_spark
from batch.gold import batch_views

log = get_logger("lambda_handoff")
TAG = config.LAMBDA_PUBLISHED_TAG


def control_rows(spark) -> list[dict]:
    return [r.asDict() for r in spark.table(config.GOLD_LAMBDA_CONTROL).collect()]


def append_control(spark, **kw):
    row = {k: kw.get(k) for k in GOLD_LAMBDA_CONTROL_FIELDS}
    row["created_at"] = dt.datetime.utcnow()          # driver chạy với TZ UTC; session Spark cũng UTC
    schema = ", ".join(f"`{k}` {v}" for k, v in GOLD_LAMBDA_CONTROL_FIELDS.items())
    spark.createDataFrame([tuple(row[k] for k in GOLD_LAMBDA_CONTROL_FIELDS)], schema) \
         .writeTo(config.GOLD_LAMBDA_CONTROL).append()
    log.info("[control] %s %s t_batch=%s", row["handoff_id"], row["status"], row["t_batch"])


def tag_snapshot(spark, table: str) -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = '{TAG}'").collect()
    return int(rows[0]["snapshot_id"]) if rows else None


def recover(spark, t_published: dt.datetime):
    rows = control_rows(spark)
    for p in lc.pending_handoffs(rows):
        action = lc.recovery_action(p, tag_snapshot(spark, config.GOLD_TRAFFIC_5MIN),
                                    tag_snapshot(spark, config.GOLD_TRAFFIC_HOURLY), t_published)
        append_control(spark, **{**p, "status": action,
                                 "message": f"recovered from {p['status']} after crash -> {action}"})
        if action == lc.PUBLISHED:
            t_published = max(t_published, lc.naive_utc(p["t_batch"]))
    return t_published


def run_checks(spark, s5: int, sh: int, lo: dt.datetime, hi: dt.datetime, allow_rt_gap: bool) -> dict:
    """Kiểm tra trên đúng snapshot sẽ công bố. lo/hi: datetime UTC naive, hi = T_new."""
    b5 = spark.sql(f"SELECT * FROM {config.GOLD_TRAFFIC_5MIN} VERSION AS OF {s5}") \
              .filter((F.col("window_start_utc") >= F.lit(lo)) & (F.col("window_start_utc") < F.lit(hi)))
    bh = spark.sql(f"SELECT * FROM {config.GOLD_TRAFFIC_HOURLY} VERSION AS OF {sh}") \
              .filter((F.col("hour_utc") >= F.lit(lo)) & (F.col("hour_utc") < F.lit(hi)))
    silver_n = spark.table(config.SILVER_TRAFFIC_CLEANED) \
                    .filter((F.col("traffic_utc") >= F.lit(lo)) & (F.col("traffic_utc") < F.lit(hi))).count()
    agg = b5.agg(F.count("*").alias("rows"), F.countDistinct("link_id", "window_start_utc").alias("keys"),
                 F.coalesce(F.sum("n_records"), F.lit(0)).alias("n")).collect()[0]
    # 1 giờ = tổng 5 phút, chỉ so các giờ nằm trọn trong [lo, T_new)
    hi_hour = hi.replace(minute=0)
    h5 = b5.filter(F.col("window_start_utc") < F.lit(hi_hour)).agg(F.coalesce(F.sum("n_records"), F.lit(0))).collect()[0][0]
    hh = bh.filter(F.col("hour_utc") < F.lit(hi_hour)).agg(F.coalesce(F.sum("n_records"), F.lit(0))).collect()[0][0]
    rt = spark.table(config.SPEED_TRAFFIC_5MIN) \
              .filter((F.col("window_start_utc") >= F.lit(lo)) & (F.col("window_start_utc") < F.lit(hi)))
    rt_only = rt.select("link_id", "window_start_utc").join(b5.select("link_id", "window_start_utc"),
                                                            ["link_id", "window_start_utc"], "left_anti").count()
    cmp = (rt.alias("r").join(b5.alias("b"), ["link_id", "window_start_utc"])
             .agg(F.count("*").alias("both"), F.sum(F.abs(F.col("b.n_records") - F.col("r.n_records"))).alias("diff"))).collect()[0]
    c = {"range": [str(lo), str(hi)], "silver_rows": silver_n, "batch_sum_n_records": int(agg["n"]),
         "batch_rows": agg["rows"], "batch_keys": agg["keys"], "hourly_sum_n_records": int(hh), "fivemin_sum_full_hours": int(h5),
         "rt_windows_compared": cmp["both"], "rt_diff_records": int(cmp["diff"] or 0), "rt_windows_without_batch": rt_only}
    c["complete"] = c["batch_sum_n_records"] == silver_n
    c["unique_grain"] = agg["rows"] == agg["keys"]
    c["hourly_consistent"] = c["hourly_sum_n_records"] == c["fivemin_sum_full_hours"]
    c["covers_realtime"] = rt_only == 0 or allow_rt_gap
    c["ok"] = c["complete"] and c["unique_grain"] and c["hourly_consistent"] and c["covers_realtime"]
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--until", default="auto", help="T_batch mới (UTC, làm tròn xuống 5 phút) hoặc 'auto'")
    ap.add_argument("--lateness-minutes", type=int, default=int(config.SPEED_WATERMARK.split()[0]),
                    help="khoảng an toàn tính từ event_time lớn nhất của Silver (mặc định = watermark Speed Layer)")
    ap.add_argument("--from-day", help="ép ngày UTC bắt đầu recompute/kiểm tra (backfill, sửa dữ liệu muộn)")
    ap.add_argument("--no-recompute", action="store_true", help="không ghi Batch View; kiểm tra + công bố snapshot hiện tại")
    ap.add_argument("--correction", action="store_true", help="cho phép công bố lại với T_batch giữ nguyên")
    ap.add_argument("--allow-rt-without-batch", action="store_true")
    ap.add_argument("--fail-at", choices=["after_prepare", "after_commit", "after_validate", "after_tag"],
                    help="giả lập crash để kiểm thử")
    args = ap.parse_args()

    spark = build_spark("lambda_handoff")
    lock = lc.AdvisoryLock(spark, config.LAMBDA_LOCK_KEY)
    if not lock.acquire():
        log.error("[handoff] một handoff khác đang chạy (advisory lock) -> thoát"); sys.exit(2)
    try:
        sys.exit(run(spark, args))
    finally:
        lock.release()


def run(spark, args) -> int:
    t_old = recover(spark, lc.current_t_batch(control_rows(spark)))
    silver = spark.table(config.SILVER_TRAFFIC_CLEANED)
    t_new, safe = lc.choose_t_new(args.until, silver.agg(F.max("traffic_utc")).collect()[0][0], args.lateness_minutes)
    reason = lc.validate_move(t_old, t_new, safe, args.correction)
    log.info("[handoff] T_old=%s T_new=%s safe_bound=%s", t_old, t_new, safe)
    if reason:
        log.warning("[handoff] không công bố: %s", reason)
        return 0 if t_new == t_old and not args.correction else 1      # chưa có cửa sổ mới: không phải lỗi

    if args.from_day:
        start_day = dt.date.fromisoformat(args.from_day)
    elif t_old == lc.EPOCH:                     # lần đầu: từ ngày nhỏ nhất có trong Silver
        start_day = lc.naive_utc(silver.agg(F.min("traffic_utc")).collect()[0][0]).date()
    else:
        start_day = t_old.date()
    end_day = (t_new - dt.timedelta(seconds=1)).date()
    hid = f"handoff-{uuid.uuid4().hex[:12]}"
    base = {"handoff_id": hid, "t_batch_prev": t_old, "t_batch": t_new}
    append_control(spark, **base, status=lc.PREPARING, message=f"recompute {start_day}..{end_day}")
    if args.fail_at == "after_prepare":
        raise SystemExit("[handoff] giả lập crash sau PREPARING")

    # 2. Commit Batch View (nhánh main) hoặc dùng snapshot hiện tại
    if args.no_recompute:
        stats = {"batch_run_id": None, "snapshot_5min": batch_views.current_snapshot(spark, config.GOLD_TRAFFIC_5MIN),
                 "snapshot_hourly": batch_views.current_snapshot(spark, config.GOLD_TRAFFIC_HOURLY)}
    else:
        stats = batch_views.run(spark, start_day, end_day, batch_run_id=hid)
    s5, sh = stats["snapshot_5min"], stats["snapshot_hourly"]
    if args.fail_at == "after_commit":
        raise SystemExit("[handoff] giả lập crash sau commit Batch View")

    # 3. Kiểm tra trên đúng snapshot sẽ công bố, cho toàn bộ khoảng đã recompute
    checks = run_checks(spark, s5, sh, dt.datetime.combine(start_day, dt.time()), t_new, args.allow_rt_without_batch)
    checks["batch_stats"] = stats
    log.info("[handoff] checks %s", json.dumps(checks, default=str))
    common = {**base, "batch_run_id": stats.get("batch_run_id"), "snapshot_5min": s5, "snapshot_hourly": sh,
              "checks": json.dumps(checks, default=str)}
    if not checks["ok"]:
        append_control(spark, **common, status=lc.FAILED, message="checks failed")
        return 1
    append_control(spark, **common, status=lc.VALIDATED)
    if args.fail_at == "after_validate":
        raise SystemExit("[handoff] giả lập crash sau VALIDATED, trước dời tag")

    # 5. Dời tag về snapshot đã kiểm chứng (Trino đọc qua tag)
    spark.sql(f"ALTER TABLE {config.GOLD_TRAFFIC_5MIN} REPLACE TAG `{TAG}` AS OF VERSION {s5}")
    spark.sql(f"ALTER TABLE {config.GOLD_TRAFFIC_HOURLY} REPLACE TAG `{TAG}` AS OF VERSION {sh}")
    if args.fail_at == "after_tag":
        raise SystemExit("[handoff] giả lập crash sau dời tag, trước PUBLISHED")

    # 6. Publish. Kiểm tra lại T_batch ngay trước khi ghi (đang giữ khoá nên không ai chen vào)
    t_now = lc.current_t_batch(control_rows(spark))
    if t_new < t_now or (t_new == t_now and not args.correction):
        append_control(spark, **common, status=lc.FAILED, message=f"T_batch đã là {t_now} -> không công bố {t_new}")
        return 1
    append_control(spark, **common, status=lc.PUBLISHED)
    log.info("[handoff] PUBLISHED T_batch = %s (5min snapshot %s, hourly snapshot %s)", t_new, s5, sh)
    return 0


if __name__ == "__main__":
    main()
