# Tạo namespace + bảng Gold trên Iceberg. Idempotent. Một quy ước đặt tên chung (mô hình sao của gold-dev):
#   1. dim_* / fact_* định nghĩa trong gold.sql (cùng thư mục, của nhánh gold-dev): CREATE IF NOT EXISTS.
#      Dữ liệu do batch/transformation/silver_to_gold.py (gold-dev) nạp; free-flow nằm ở dim_road_link.free_flow_speed_mph
#      (thay bảng gold.link_baseline cũ).
#   2. Bảng Lambda dưới đây, đặt tên cùng quy ước: fact_traffic_5min, fact_traffic_hourly (Batch View, có date_key /
#      hour_key theo giờ New York, cột *_mph / *_sec) và lambda_control (nhật ký handoff).
#   - Bảng đã có thì TIẾN HOÁ schema: đổi tên cột cũ, thêm cột thiếu, bỏ cột liệt kê trong `drops`, sắp lại thứ tự cột
#     theo schema (chỉ đổi metadata: file dữ liệu và snapshot cũ còn nguyên, time travel vẫn đọc được). Không DROP TABLE.
#   - Bảng cũ trước khi đổi tên (gold.traffic_5min_batch, gold.traffic_hourly, gold.link_baseline) KHÔNG bị xoá ở đây.
#   - Bảng Batch View có tag `published` (Trino đọc Batch View qua tag này - xem docs/lambda_handoff.md).
# Chạy: docker exec traffic-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/ddl/gold.py
import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from common import config
from common.schemas import GOLD_LAMBDA_CONTROL_FIELDS, GOLD_TRAFFIC_5MIN_FIELDS, GOLD_TRAFFIC_HOURLY_FIELDS, to_ddl
from common.spark_session import build_spark

BASE_PROPS = {
    "format-version": "2",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
}


def create_table(spark: SparkSession, table: str, fields: dict, partition_by: str | None = None,
                 props: dict | None = None, renames: dict | None = None, drops: tuple = ()):
    partition = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    tblprops = ", ".join(f"'{k}' = '{v}'" for k, v in {**BASE_PROPS, **(props or {})}.items())
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({to_ddl(fields)}) USING iceberg {partition} TBLPROPERTIES ({tblprops})")
    evolve_schema(spark, table, fields, renames or {}, drops)


def _columns(spark: SparkSession, table: str) -> list[str]:
    return spark.table(table).columns


def evolve_schema(spark: SparkSession, table: str, fields: dict, renames: dict, drops: tuple = ()) -> bool:
    """Bảng có sẵn từ phiên bản trước: đổi tên cột cũ -> mới, thêm cột còn thiếu, bỏ các cột trong `drops`
    (chỉ cột không còn trong schema; không xoá file dữ liệu), rồi sắp thứ tự cột đúng như `fields`
    (sink Iceberg của Spark kiểm tra thứ tự cột). Trả True nếu schema đã đổi."""
    have = set(_columns(spark, table))
    changed = False
    for old, new in renames.items():
        if old in have and new not in have:
            spark.sql(f"ALTER TABLE {table} RENAME COLUMN `{old}` TO `{new}`")
            have = (have - {old}) | {new}
            changed = True
    for col, typ in fields.items():
        if col not in have:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN `{col}` {typ}")
            changed = True
    for col in drops:
        if col in have and col not in fields:
            spark.sql(f"ALTER TABLE {table} DROP COLUMN `{col}`")
            changed = True
    wanted = list(fields)
    current = [c for c in _columns(spark, table) if c in fields]
    if current != wanted:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{wanted[0]}` FIRST")
        for prev, col in zip(wanted, wanted[1:]):
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{col}` AFTER `{prev}`")
        changed = True
    return changed


# Cột suy ra được, bỏ khỏi bảng 5 phút (view serving tính khi đọc) - xem common/schemas.py
DERIVED_5MIN = ("window_end_utc", "n_invalid_other", "min_speed", "max_speed", "avg_speed", "quality_flag")


def _ref(spark: SparkSession, table: str, name: str) -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = '{name}'").collect()
    return int(rows[0]["snapshot_id"]) if rows else None


def ensure_tag(spark: SparkSession, table: str, tag: str):
    """Tag cần một snapshot. Bảng mới tạo chưa có snapshot -> commit một lần ghi rỗng rồi tạo tag (Batch View rỗng).
    Đọc theo tag dùng schema CỦA SNAPSHOT đó: sau khi tiến hoá schema, nếu tag đang trỏ đúng snapshot hiện hành (không có
    dữ liệu chưa kiểm chứng ở giữa) thì commit rỗng để có snapshot mang schema mới rồi dời tag sang - dữ liệu y hệt."""
    if spark.sql(f"SELECT count(*) AS n FROM {table}.snapshots").collect()[0]["n"] == 0:
        spark.sql(f"INSERT INTO {table} SELECT * FROM {table} WHERE false")
    tagged, main = _ref(spark, table, tag), _ref(spark, table, "main")
    if tagged is None:
        spark.sql(f"ALTER TABLE {table} CREATE TAG IF NOT EXISTS `{tag}`")
        return
    # Đọc time travel dùng schema của snapshot -> so danh sách cột đọc qua tag với cột hiện hành của bảng
    tag_cols = spark.sql(f"SELECT * FROM {table} VERSION AS OF {tagged} LIMIT 0").columns
    if tagged == main and tag_cols != spark.table(table).columns:
        spark.sql(f"INSERT INTO {table} SELECT * FROM {table} WHERE false")
        spark.sql(f"ALTER TABLE {table} REPLACE TAG `{tag}` AS OF VERSION {_ref(spark, table, 'main')}")


def run_sql_file(spark: SparkSession, path: Path):
    """Chạy các câu lệnh trong gold.sql (mô hình sao của gold-dev). File chỉ có comment cả dòng và không có ';'
    trong chuỗi. Tên `lakehouse.gold` trong file được thay bằng config.GOLD_NAMESPACE (để chạy thử cô lập)."""
    sql = "\n".join(line for line in path.read_text(encoding="utf-8").splitlines() if not line.lstrip().startswith("--"))
    sql = sql.replace("lakehouse.gold", config.GOLD_NAMESPACE)
    for statement in sql.split(";"):
        if statement.strip():
            spark.sql(statement.strip())


# Bảng mô hình sao của gold.sql: --recreate (của gold-dev) xoá + PURGE rồi tạo lại đúng schema hiện tại.
STAR_TABLES = ("fact_congestion_hourly", "fact_weather_hourly", "fact_traffic_reading", "dim_road_link",
               "dim_weather_condition", "dim_weather_grid", "dim_hour", "dim_date")


def main():
    ap = argparse.ArgumentParser(description="Tạo bảng Gold (gold.sql + bảng Lambda)")
    ap.add_argument("--recreate", action="store_true",
                    help="xoá/PURGE 8 bảng mô hình sao của gold.sql trước khi tạo lại (KHÔNG đụng bảng Lambda)")
    args = ap.parse_args()
    spark = build_spark("ddl_gold")
    if args.recreate:
        for name in STAR_TABLES:
            spark.sql(f"DROP TABLE IF EXISTS {config.GOLD_NAMESPACE}.{name} PURGE")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.GOLD_NAMESPACE}")
    run_sql_file(spark, Path(__file__).with_suffix(".sql"))     # dim_* / fact_* của gold-dev
    # Schema chỉ chứa VIEW Trino (jobs/serving/trino_views.sql). Spark tạo schema để Trino không cần quyền tạo schema.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.SERVING_SCHEMA}")
    # Phân vùng theo NGÀY UTC: Batch View ghi lại từng ngày (overwrite theo khoảng) -> chạy lại không sinh trùng
    create_table(spark, config.GOLD_TRAFFIC_5MIN, GOLD_TRAFFIC_5MIN_FIELDS, "days(window_start_utc)", drops=DERIVED_5MIN)
    create_table(spark, config.GOLD_TRAFFIC_HOURLY, GOLD_TRAFFIC_HOURLY_FIELDS, "days(hour_start_utc)")
    create_table(spark, config.GOLD_LAMBDA_CONTROL, GOLD_LAMBDA_CONTROL_FIELDS,
                 renames={"t_batch_prev": "batch_state_prev", "t_batch": "batch_state"})
    for t in (config.GOLD_TRAFFIC_5MIN, config.GOLD_TRAFFIC_HOURLY):
        ensure_tag(spark, t, config.LAMBDA_PUBLISHED_TAG)
    spark.sql(f"SHOW TABLES IN {config.GOLD_NAMESPACE}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
