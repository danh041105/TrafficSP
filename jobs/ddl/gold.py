# Tạo namespace + bảng Gold trên Iceberg. Idempotent. Hai nhóm bảng, không trùng tên:
#   1. Mô hình sao (dim_* / fact_*): định nghĩa trong gold.sql (cùng thư mục), chỉ CREATE IF NOT EXISTS.
#   2. Bảng Lambda (Batch View 5 phút / 1 giờ, link_baseline, lambda_control) dưới đây:
#   - CREATE TABLE IF NOT EXISTS; bảng đã có thì chỉ TIẾN HOÁ schema (thêm cột thiếu / đổi tên cột cũ), không DROP.
#   - Bảng Batch View có tag `published` (Trino đọc Batch View qua tag này - xem docs/lambda_handoff.md).
# Chạy: docker exec traffic-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/ddl/gold.py
from pathlib import Path

from pyspark.sql import SparkSession

from common import config
from common.schemas import (GOLD_LAMBDA_CONTROL_FIELDS, GOLD_LINK_BASELINE_FIELDS, GOLD_TRAFFIC_5MIN_FIELDS,
                            GOLD_TRAFFIC_HOURLY_FIELDS, to_ddl)
from common.spark_session import build_spark

BASE_PROPS = {
    "format-version": "2",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
}


def create_table(spark: SparkSession, table: str, fields: dict, partition_by: str | None = None,
                 props: dict | None = None, renames: dict | None = None):
    partition = f"PARTITIONED BY ({partition_by})" if partition_by else ""
    tblprops = ", ".join(f"'{k}' = '{v}'" for k, v in {**BASE_PROPS, **(props or {})}.items())
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({to_ddl(fields)}) USING iceberg {partition} TBLPROPERTIES ({tblprops})")
    evolve_schema(spark, table, fields, renames or {})


def evolve_schema(spark: SparkSession, table: str, fields: dict, renames: dict) -> bool:
    """Bảng có sẵn từ phiên bản trước: đổi tên cột cũ -> mới, thêm cột còn thiếu. Không xoá cột, không xoá dữ liệu.
    Trả True nếu schema đã đổi."""
    have = {r["col_name"] for r in spark.sql(f"DESCRIBE TABLE {table}").collect() if r["col_name"] and not r["col_name"].startswith("#")}
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
    return changed


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
    """Chạy các câu lệnh trong gold.sql (mô hình sao). File chỉ có comment cả dòng và không có ';' trong chuỗi."""
    sql = "\n".join(line for line in path.read_text(encoding="utf-8").splitlines() if not line.lstrip().startswith("--"))
    for statement in sql.split(";"):
        if statement.strip():
            spark.sql(statement.strip())


def main():
    spark = build_spark("ddl_gold")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.GOLD_NAMESPACE}")
    run_sql_file(spark, Path(__file__).with_suffix(".sql"))     # dim_* / fact_* (gold.sql)
    # Schema chỉ chứa VIEW Trino (jobs/serving/trino_views.sql). Spark tạo schema để Trino không cần quyền tạo schema.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.SERVING_SCHEMA}")
    # Phân vùng theo NGÀY UTC: Batch View ghi lại từng ngày (overwritePartitions) -> chạy lại không sinh trùng
    create_table(spark, config.GOLD_TRAFFIC_5MIN, GOLD_TRAFFIC_5MIN_FIELDS, "days(window_start_utc)")
    create_table(spark, config.GOLD_TRAFFIC_HOURLY, GOLD_TRAFFIC_HOURLY_FIELDS, "days(hour_utc)",
                 renames={"computed_ts": "processed_at"})
    create_table(spark, config.GOLD_LINK_BASELINE, GOLD_LINK_BASELINE_FIELDS)
    create_table(spark, config.GOLD_LAMBDA_CONTROL, GOLD_LAMBDA_CONTROL_FIELDS)
    for t in (config.GOLD_TRAFFIC_5MIN, config.GOLD_TRAFFIC_HOURLY):
        ensure_tag(spark, t, config.LAMBDA_PUBLISHED_TAG)
    spark.sql(f"SHOW TABLES IN {config.GOLD_NAMESPACE}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
