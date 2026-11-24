"""Bảo trì Iceberg cho bảng ghi liên tục (Speed Layer) và Batch View.

Vì sao cần: mỗi micro-batch của Speed Layer = 1 commit = 1 snapshot + vài file Parquet nhỏ (đo được ~10-18 KB/file).
Không bảo trì -> số file và số snapshot tăng tuyến tính theo thời gian -> Trino lập kế hoạch chậm, quét nhiều split.

Các bước (mặc định chỉ chạy bước KHÔNG xoá dữ liệu):
  --compact          rewrite_data_files (gộp file nhỏ theo phân vùng, mục tiêu 128 MB) + rewrite_manifests.
                     An toàn khi Speed Layer đang chạy: commit xung đột thì Iceberg tự thử lại (commit.retry.num-retries);
                     dữ liệu logic không đổi. Đã chạy thử song song với Speed Layer: 32 file -> 1, không mất/trùng dòng.
  --expire-snapshots XOÁ snapshot cũ hơn --older-than-hours (giữ tối thiểu --retain-last) và các file chỉ snapshot đó dùng.
                     MẤT khả năng time travel về trước mốc đó. Snapshot được tag/branch trỏ tới (vd tag 'published' của
                     Batch View) luôn được giữ. Chỉ chạy khi đã được chấp thuận; cần thêm --yes-delete-old-snapshots.
Không có bước xoá file mồ côi (remove_orphan_files): rủi ro xoá nhầm file của commit đang chạy; làm tay khi cần.

Chạy: spark-submit .../iceberg_maintenance.py --tables speed --compact
      spark-submit .../iceberg_maintenance.py --tables speed,gold --expire-snapshots --older-than-hours 72 --yes-delete-old-snapshots
"""
import argparse
import datetime as dt

from common import config
from common.logger import get_logger
from common.spark_session import build_spark

log = get_logger("iceberg_maintenance")
GROUPS = {
    "speed": [config.SPEED_TRAFFIC_5MIN, config.SPEED_TRAFFIC_LATEST, config.SPEED_WEATHER_OBS],
    "gold": [config.GOLD_TRAFFIC_5MIN, config.GOLD_TRAFFIC_HOURLY],
}


def short(table: str) -> str:
    return table.split(".", 1)[1]        # bỏ tên catalog cho tham số procedure


def stats(spark, table: str) -> dict:
    f = spark.sql(f"SELECT count(*) AS files, coalesce(avg(file_size_in_bytes), 0) AS avg_b FROM {table}.files").collect()[0]
    s = spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").collect()[0]
    return {"files": f["files"], "avg_kb": round(f["avg_b"] / 1024, 1), "snapshots": s["n"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default="speed", help="nhóm bảng: speed,gold")
    ap.add_argument("--compact", action="store_true")
    ap.add_argument("--target-file-mb", type=int, default=128)
    ap.add_argument("--expire-snapshots", action="store_true")
    ap.add_argument("--older-than-hours", type=int, default=72)
    ap.add_argument("--retain-last", type=int, default=100)
    ap.add_argument("--yes-delete-old-snapshots", action="store_true")
    args = ap.parse_args()
    if args.expire_snapshots and not args.yes_delete_old_snapshots:
        raise SystemExit("--expire-snapshots xoá snapshot/file cũ: thêm --yes-delete-old-snapshots để xác nhận")

    spark = build_spark("iceberg_maintenance")
    tables = [t for g in args.tables.split(",") for t in GROUPS[g.strip()]]
    for t in tables:
        before = stats(spark, t)
        if args.compact:
            r = spark.sql(f"CALL lakehouse.system.rewrite_data_files(table => '{short(t)}', options => map("
                          f"'min-input-files', '2', 'target-file-size-bytes', '{args.target_file_mb * 2 ** 20}'))").collect()[0]
            spark.sql(f"CALL lakehouse.system.rewrite_manifests(table => '{short(t)}')").collect()
            log.info("[compact] %s: rewrote %s files -> %s", t, r["rewritten_data_files_count"], r["added_data_files_count"])
        if args.expire_snapshots:
            cutoff = (dt.datetime.utcnow() - dt.timedelta(hours=args.older_than_hours)).strftime("%Y-%m-%d %H:%M:%S")
            r = spark.sql(f"CALL lakehouse.system.expire_snapshots(table => '{short(t)}', older_than => TIMESTAMP '{cutoff}', "
                          f"retain_last => {args.retain_last})").collect()[0]
            log.info("[expire] %s: %s", t, r.asDict())
        log.info("[stats] %s before=%s after=%s", t, before, stats(spark, t))
    spark.stop()


if __name__ == "__main__":
    main()
